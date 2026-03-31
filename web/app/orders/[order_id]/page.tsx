import { cookies } from "next/headers";
import { redirect, notFound } from "next/navigation";
import { queryOne, query } from "@/lib/db";
import Link from "next/link";

interface Order {
  order_id: number;
  customer_id: number;
  order_datetime: string;
  order_subtotal: number;
  shipping_fee: number;
  tax_amount: number;
  order_total: number;
  payment_method: string;
  device_type: string;
  fulfilled: number;
  is_fraud: number;
  risk_score: number;
}

interface LineItem {
  order_item_id: number;
  product_name: string;
  category: string;
  quantity: number;
  unit_price: number;
  line_total: number;
}

interface Prediction {
  late_delivery_probability: number;
  predicted_late_delivery: number;
  prediction_timestamp: string;
}

export default async function OrderDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ order_id: string }>;
  searchParams: Promise<{ success?: string }>;
}) {
  const cookieStore = await cookies();
  const customerId = cookieStore.get("customer_id")?.value;
  if (!customerId) redirect("/select-customer");

  const { order_id } = await params;
  const { success } = await searchParams;

  const order = await queryOne<Order>(
    "SELECT * FROM orders WHERE order_id = $1 AND customer_id = $2",
    [order_id, customerId]
  );
  if (!order) notFound();

  const items = await query<LineItem>(
    `SELECT oi.order_item_id, p.product_name, p.category, oi.quantity, oi.unit_price, oi.line_total
     FROM order_items oi
     JOIN products p ON p.product_id = oi.product_id
     WHERE oi.order_id = $1
     ORDER BY oi.order_item_id`,
    [order_id]
  );

  const prediction = await queryOne<Prediction>(
    "SELECT late_delivery_probability, predicted_late_delivery, prediction_timestamp FROM order_predictions WHERE order_id = $1",
    [order_id]
  );

  return (
    <div>
      <div className="page-header">
        <h1>Order #{order.order_id}</h1>
        <p>
          {order.order_datetime?.slice(0, 16).replace("T", " ") ?? "—"} ·{" "}
          <span className={`badge ${order.fulfilled ? "badge-success" : "badge-warning"}`}>
            {order.fulfilled ? "Fulfilled" : "Pending"}
          </span>
        </p>
      </div>

      {success && (
        <div className="alert alert-success">
          Order placed successfully! It has been saved to the database.
        </div>
      )}

      <div className="card">
        <div className="card-title">Line Items</div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Product</th>
                <th>Category</th>
                <th>Qty</th>
                <th>Unit Price</th>
                <th>Line Total</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.order_item_id}>
                  <td>{item.product_name}</td>
                  <td>{item.category}</td>
                  <td>{item.quantity}</td>
                  <td>${item.unit_price.toFixed(2)}</td>
                  <td>${item.line_total.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div style={{ marginTop: "16px", borderTop: "1px solid var(--border)", paddingTop: "12px" }}>
          <table style={{ width: "300px", marginLeft: "auto" }}>
            <tbody>
              <tr>
                <td style={{ color: "var(--muted)", padding: "4px 0" }}>Subtotal</td>
                <td style={{ textAlign: "right" }}>${order.order_subtotal?.toFixed(2) ?? "0.00"}</td>
              </tr>
              <tr>
                <td style={{ color: "var(--muted)", padding: "4px 0" }}>Shipping</td>
                <td style={{ textAlign: "right" }}>${order.shipping_fee?.toFixed(2) ?? "0.00"}</td>
              </tr>
              <tr>
                <td style={{ color: "var(--muted)", padding: "4px 0" }}>Tax</td>
                <td style={{ textAlign: "right" }}>${order.tax_amount?.toFixed(2) ?? "0.00"}</td>
              </tr>
              <tr style={{ fontWeight: 700, fontSize: "16px" }}>
                <td style={{ paddingTop: "8px" }}>Total</td>
                <td style={{ textAlign: "right", paddingTop: "8px" }}>${order.order_total?.toFixed(2) ?? "0.00"}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {prediction ? (
        <div className="card">
          <div className="card-title">ML Prediction — Late Delivery Risk</div>
          <div style={{ display: "flex", gap: "32px", alignItems: "center" }}>
            <div>
              <div style={{ color: "var(--muted)", fontSize: "13px", marginBottom: "4px" }}>Probability</div>
              <div style={{ fontSize: "28px", fontWeight: 700, color: prediction.late_delivery_probability > 0.5 ? "var(--danger)" : "var(--success)" }}>
                {(prediction.late_delivery_probability * 100).toFixed(1)}%
              </div>
            </div>
            <div>
              <div style={{ color: "var(--muted)", fontSize: "13px", marginBottom: "4px" }}>Prediction</div>
              <span className={`badge ${prediction.predicted_late_delivery ? "badge-danger" : "badge-success"}`} style={{ fontSize: "14px" }}>
                {prediction.predicted_late_delivery ? "Late" : "On Time"}
              </span>
            </div>
            <div>
              <div style={{ color: "var(--muted)", fontSize: "13px", marginBottom: "4px" }}>Scored at</div>
              <div style={{ fontSize: "13px" }}>{prediction.prediction_timestamp?.slice(0, 19).replace("T", " ") ?? "—"}</div>
            </div>
          </div>
        </div>
      ) : (
        <div className="card">
          <div className="alert alert-info" style={{ margin: 0 }}>
            No ML prediction yet for this order.{" "}
            <Link href="/scoring">Run Scoring →</Link> to generate one.
          </div>
        </div>
      )}

      <div style={{ marginTop: "8px" }}>
        <Link href="/orders" className="btn btn-secondary">← Back to Orders</Link>
      </div>
    </div>
  );
}
