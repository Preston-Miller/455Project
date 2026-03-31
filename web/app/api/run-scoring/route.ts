import { NextResponse } from "next/server";
import { query, withTransaction, queryWithClient } from "@/lib/db";

interface LiveOrder {
  order_id: number;
  order_datetime: string;
  birthdate: string;
  num_items: number;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function sigmoid(z: number): number {
  return 1 / (1 + Math.exp(-z));
}

export async function POST() {
  try {
    const liveOrders = await query<LiveOrder>(
      `SELECT
         o.order_id,
         o.order_datetime,
         c.birthdate,
         oi_agg.num_items
       FROM orders o
       JOIN customers c ON c.customer_id = o.customer_id
       JOIN (
         SELECT order_id, SUM(quantity)::int AS num_items
         FROM order_items
         GROUP BY order_id
       ) oi_agg ON oi_agg.order_id = o.order_id
       WHERE o.fulfilled = 0`
    );

    if (liveOrders.length === 0) {
      return NextResponse.json({
        success: true,
        count: 0,
        output: "No unfulfilled orders found.",
      });
    }

    const nowYear = new Date().getUTCFullYear();
    const scored = liveOrders.map((order) => {
      const orderDate = new Date(order.order_datetime);
      const birthDate = new Date(order.birthdate);

      const customerAge = Number.isFinite(birthDate.getUTCFullYear())
        ? nowYear - birthDate.getUTCFullYear()
        : 35;
      const orderDow = Number.isFinite(orderDate.getUTCDay()) ? orderDate.getUTCDay() : 1;
      const orderMonth = Number.isFinite(orderDate.getUTCMonth()) ? orderDate.getUTCMonth() + 1 : 6;

      // Lightweight deployed scoring model for late-delivery risk.
      const numItemsScaled = clamp(order.num_items, 1, 12) / 12;
      const youngerCustomerRisk = clamp((40 - customerAge) / 25, 0, 1);
      const weekendRisk = orderDow === 0 || orderDow === 6 ? 1 : 0;
      const holidayRisk = orderMonth >= 11 ? 1 : 0;

      const z =
        -2.1 +
        2.4 * numItemsScaled +
        1.1 * youngerCustomerRisk +
        0.8 * weekendRisk +
        0.6 * holidayRisk;

      const lateDeliveryProbability = clamp(sigmoid(z), 0.01, 0.99);
      const predictedLateDelivery = lateDeliveryProbability >= 0.5 ? 1 : 0;

      return {
        order_id: order.order_id,
        late_delivery_probability: lateDeliveryProbability,
        predicted_late_delivery: predictedLateDelivery,
      };
    });

    const predictionTimestamp = new Date().toISOString();

    await withTransaction(async (client) => {
      for (const row of scored) {
        await queryWithClient(
          client,
          `INSERT INTO order_predictions
            (order_id, late_delivery_probability, predicted_late_delivery, prediction_timestamp)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (order_id) DO UPDATE
           SET late_delivery_probability = EXCLUDED.late_delivery_probability,
               predicted_late_delivery = EXCLUDED.predicted_late_delivery,
               prediction_timestamp = EXCLUDED.prediction_timestamp`,
          [
            row.order_id,
            row.late_delivery_probability,
            row.predicted_late_delivery,
            predictionTimestamp,
          ]
        );
      }
    });

    return NextResponse.json({
      success: true,
      count: scored.length,
      output: `Scored ${scored.length} unfulfilled orders and upserted predictions.`,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown server error";
    return NextResponse.json({ success: false, error: message }, { status: 500 });
  }
}
