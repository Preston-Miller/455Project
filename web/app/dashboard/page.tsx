import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { query, queryOne } from "@/lib/db";
import Link from "next/link";

interface Customer {
  customer_id: number;
  full_name: string;
  email: string;
  city: string;
  state: string;
  loyalty_tier: string;
  customer_segment: string;
}

interface Stats {
  total_orders: number;
  total_spend: number;
}

interface RecentOrder {
  order_id: number;
  order_datetime: string;
  order_total: number;
  fulfilled: number;
  item_count: number;
}

export default async function DashboardPage() {
  const cookieStore = await cookies();
  const customerId = cookieStore.get("customer_id")?.value;
  if (!customerId) redirect("/select-customer");

  const customer = await queryOne<Customer>(
    "SELECT customer_id, full_name, email, city, state, loyalty_tier, customer_segment FROM customers WHERE customer_id = $1",
    [customerId]
  );
  if (!customer) redirect("/select-customer");

  const stats = await queryOne<Stats>(
    `SELECT COUNT(*) AS total_orders, ROUND(COALESCE(SUM(order_total), 0), 2) AS total_spend
     FROM orders WHERE customer_id = $1`,
    [customerId]
  );

  const recentOrders = await query<RecentOrder>(
    `SELECT o.order_id, o.order_datetime, o.order_total, o.fulfilled,
            COUNT(oi.order_item_id) AS item_count
     FROM orders o
     LEFT JOIN order_items oi ON oi.order_id = o.order_id
     WHERE o.customer_id = $1
     GROUP BY o.order_id
     ORDER BY o.order_datetime DESC
     LIMIT 5`,
    [customerId]
  );

  return (
    <div>
      <div className="page-header">
        <h1>Dashboard</h1>
        <p>Summary for {customer.full_name}</p>
      </div>

      <div className="stat-grid">
        <div className="card">
          <div className="card-title">Total Orders</div>
          <div className="card-value">{stats?.total_orders ?? 0}</div>
        </div>
        <div className="card">
          <div className="card-title">Total Spend</div>
          <div className="card-value">${(stats?.total_spend ?? 0).toFixed(2)}</div>
        </div>
        <div className="card">
          <div className="card-title">Loyalty Tier</div>
          <div className="card-value">{customer.loyalty_tier}</div>
        </div>
        <div className="card">
          <div className="card-title">Segment</div>
          <div className="card-value" style={{ fontSize: "18px", paddingTop: "6px" }}>{customer.customer_segment}</div>
        </div>
      </div>

      <div className="card">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px" }}>
          <div className="card-title" style={{ marginBottom: 0 }}>Recent Orders</div>
          <Link href="/orders" className="btn btn-secondary" style={{ fontSize: "13px", padding: "6px 14px" }}>
            View All →
          </Link>
        </div>
        {recentOrders.length === 0 ? (
          <div className="empty-state">
            <h3>No orders yet</h3>
            <p>
              <Link href="/place-order">Place your first order →</Link>
            </p>
          </div>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Order ID</th>
                  <th>Date</th>
                  <th>Items</th>
                  <th>Total</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {recentOrders.map((o) => (
                  <tr key={o.order_id}>
                    <td>
                      <Link href={`/orders/${o.order_id}`}>#{o.order_id}</Link>
                    </td>
                    <td>{o.order_datetime?.slice(0, 16).replace("T", " ") ?? "—"}</td>
                    <td>{o.item_count}</td>
                    <td>${o.order_total?.toFixed(2) ?? "0.00"}</td>
                    <td>
                      <span className={`badge ${o.fulfilled ? "badge-success" : "badge-warning"}`}>
                        {o.fulfilled ? "Fulfilled" : "Pending"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="card">
        <div className="card-title">Customer Info</div>
        <table>
          <tbody>
            <tr><td style={{ color: "var(--muted)", width: "140px" }}>Name</td><td>{customer.full_name}</td></tr>
            <tr><td style={{ color: "var(--muted)" }}>Email</td><td>{customer.email}</td></tr>
            <tr><td style={{ color: "var(--muted)" }}>Location</td><td>{customer.city}, {customer.state}</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}
