import { NextResponse } from "next/server";
import { cookies } from "next/headers";
import { queryOne, queryWithClient, withTransaction } from "@/lib/db";

interface LineItem {
  product_id: number;
  quantity: number;
}

interface Product {
  product_id: number;
  price: number;
}

export async function POST(request: Request) {
  const cookieStore = await cookies();
  const customerId = cookieStore.get("customer_id")?.value;
  if (!customerId) {
    return NextResponse.json({ error: "No customer selected." }, { status: 401 });
  }

  let body: { lines: LineItem[] };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid request body." }, { status: 400 });
  }

  const { lines } = body;
  if (!lines || lines.length === 0) {
    return NextResponse.json({ error: "No line items provided." }, { status: 400 });
  }

  // Validate and price all products
  const pricedLines: Array<{ product_id: number; quantity: number; unit_price: number; line_total: number }> = [];
  for (const line of lines) {
    if (!line.product_id || line.quantity < 1) {
      return NextResponse.json({ error: "Invalid line item." }, { status: 400 });
    }
    const product = await queryOne<Product>(
      "SELECT product_id, price FROM products WHERE product_id = $1 AND is_active = 1",
      [line.product_id]
    );
    if (!product) {
      return NextResponse.json({ error: `Product ${line.product_id} not found.` }, { status: 400 });
    }
    const line_total = parseFloat((product.price * line.quantity).toFixed(2));
    pricedLines.push({ product_id: line.product_id, quantity: line.quantity, unit_price: product.price, line_total });
  }

  const order_subtotal = parseFloat(pricedLines.reduce((s, l) => s + l.line_total, 0).toFixed(2));
  const shipping_fee = 5.99;
  const tax_amount = parseFloat((order_subtotal * 0.08).toFixed(2));
  const order_total = parseFloat((order_subtotal + shipping_fee + tax_amount).toFixed(2));
  const order_datetime = new Date().toISOString();

  try {
    const order_id = await withTransaction(async (client) => {
      const inserted = await queryWithClient<{ order_id: number }>(
        client,
        `INSERT INTO orders
          (customer_id, order_datetime, payment_method, device_type, ip_country,
           promo_used, order_subtotal, shipping_fee, tax_amount, order_total,
           risk_score, is_fraud, fulfilled)
         VALUES ($1, $2, 'credit_card', 'web', 'US', 0, $3, $4, $5, $6, 0, 0, 0)
         RETURNING order_id`,
        [Number(customerId), order_datetime, order_subtotal, shipping_fee, tax_amount, order_total]
      );
      const newOrderId = inserted[0].order_id;

      for (const line of pricedLines) {
        await queryWithClient(
          client,
          `INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
           VALUES ($1, $2, $3, $4, $5)`,
          [newOrderId, line.product_id, line.quantity, line.unit_price, line.line_total]
        );
      }

      return newOrderId;
    });

    return NextResponse.json({ order_id });
  } catch (err) {
    console.error("Order insert error:", err);
    return NextResponse.json({ error: "Database error while saving order." }, { status: 500 });
  }
}
