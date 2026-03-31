import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { query, queryOne } from "@/lib/db";
import Link from "next/link";

interface Order {
  order_id: number;
  order_datetime: string;
  order_total: number;
  fulfilled: number;
  item_count: number;
}

export default async function OrdersPage() {
  const cookieStore = await cookies();
  const customerId = cookieStore.get("customer_id")?.value;
  if (!customerId) redirect("/select-customer");

  const customer = await queryOne<{ full_name: string }>(
    "SELECT full_name FROM customers WHERE customer_id = $1",
    [customerId]
  );

  const orders = await query<Order>(
    `SELECT o.order_id, o.order_datetime, o.order_total, o.fulfilled,
            COUNT(oi.order_item_id) AS item_count
     FROM orders o
     LEFT JOIN order_items oi ON oi.order_id = o.order_id
     WHERE o.customer_id = $1
     GROUP BY o.order_id
     ORDER BY o.order_datetime DESC`,
    [customerId]
  );

  return (
    <div>
      <div className="page-header">
        <h1>Order History</h1>
        <p>{customer?.full_name} — {orders.length} order{orders.length !== 1 ? "s" : ""}</p>
      </div>

      <div style={{ marginBottom: "16px" }}>
        <Link href="/place-order" className="btn btn-primary">+ Place New Order</Link>
      </div>

      {orders.length === 0 ? (
        <div className="card">
          <div className="empty-state">
            <h3>No orders yet</h3>
            <p><Link href="/place-order">Place your first order →</Link></p>
          </div>
        </div>
      ) : (
        <div className="card">
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Order ID</th>
                  <th>Date &amp; Time</th>
                  <th>Items</th>
                  <th>Total</th>
                  <th>Status</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                {orders.map((o) => (
                  <tr key={o.order_id}>
                    <td>#{o.order_id}</td>
                    <td>{o.order_datetime?.slice(0, 16).replace("T", " ") ?? "—"}</td>
                    <td>{o.item_count}</td>
                    <td>${o.order_total?.toFixed(2) ?? "0.00"}</td>
                    <td>
                      <span className={`badge ${o.fulfilled ? "badge-success" : "badge-warning"}`}>
                        {o.fulfilled ? "Fulfilled" : "Pending"}
                      </span>
                    </td>
                    <td>
                      <Link href={`/orders/${o.order_id}`}>View →</Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
