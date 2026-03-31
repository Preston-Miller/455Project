import type { Metadata } from "next";
import "./globals.css";
import NavLink from "./components/NavLink";
import { cookies } from "next/headers";
import { queryOne } from "@/lib/db";

export const metadata: Metadata = {
  title: "Shop ML App",
  description: "Chapter 17 – ML Pipeline + Order Management",
};

interface Customer {
  customer_id: number;
  full_name: string;
  email: string;
}

export default async function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  const cookieStore = await cookies();
  const customerId = cookieStore.get("customer_id")?.value;
  let customer: Customer | undefined;
  if (customerId) {
    customer = await queryOne<Customer>(
      "SELECT customer_id, full_name, email FROM customers WHERE customer_id = $1",
      [customerId]
    );
  }

  return (
    <html lang="en">
      <body>
        <div className="app-shell">
          <aside className="sidebar">
            <div className="sidebar-logo">
              Shop ML App
              <span>Chapter 17 Pipeline</span>
            </div>

            {customer && (
              <div className="customer-banner">
                <strong>{customer.full_name}</strong>
                Acting as customer #{customer.customer_id}
              </div>
            )}

            <nav>
              <div className="nav-section">Customers</div>
              <NavLink href="/select-customer">Select Customer</NavLink>
              <NavLink href="/dashboard">Dashboard</NavLink>

              <div className="nav-section">Orders</div>
              <NavLink href="/place-order">Place Order</NavLink>
              <NavLink href="/orders">Order History</NavLink>

              <div className="nav-section">ML Pipeline</div>
              <NavLink href="/warehouse/priority">Priority Queue</NavLink>
              <NavLink href="/scoring">Run Scoring</NavLink>

              <div className="nav-section">Dev</div>
              <NavLink href="/debug/schema">DB Schema</NavLink>
            </nav>
          </aside>

          <main className="main-content">{children}</main>
        </div>
      </body>
    </html>
  );
}
