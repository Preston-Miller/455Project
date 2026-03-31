import { query } from "@/lib/db";
import { cookies } from "next/headers";
import { redirect } from "next/navigation";

interface Customer {
  customer_id: number;
  full_name: string;
  email: string;
  city: string;
  state: string;
  loyalty_tier: string;
}

async function selectCustomer(formData: FormData) {
  "use server";
  const id = formData.get("customer_id") as string;
  if (!id) return;
  const cookieStore = await cookies();
  cookieStore.set("customer_id", id, { path: "/", httpOnly: true, maxAge: 60 * 60 * 24 });
  redirect("/dashboard");
}

export default async function SelectCustomerPage() {
  const customers = await query<Customer>(
    "SELECT customer_id, full_name, email, city, state, loyalty_tier FROM customers WHERE is_active = 1 ORDER BY full_name"
  );

  return (
    <div>
      <div className="page-header">
        <h1>Select Customer</h1>
        <p>Choose a customer to act as. No login required.</p>
      </div>

      <div className="card">
        <form action={selectCustomer}>
          <div className="form-group">
            <label htmlFor="customer_id">Customer</label>
            <select name="customer_id" id="customer_id" required>
              <option value="">— choose a customer —</option>
              {customers.map((c) => (
                <option key={c.customer_id} value={c.customer_id}>
                  {c.full_name} — {c.email} ({c.city}, {c.state})
                </option>
              ))}
            </select>
          </div>
          <button type="submit" className="btn btn-primary">
            Select &amp; Go to Dashboard →
          </button>
        </form>
      </div>

      <div className="card">
        <div className="card-title">All Customers ({customers.length})</div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Email</th>
                <th>City</th>
                <th>State</th>
                <th>Tier</th>
              </tr>
            </thead>
            <tbody>
              {customers.map((c) => (
                <tr key={c.customer_id}>
                  <td>{c.customer_id}</td>
                  <td>{c.full_name}</td>
                  <td>{c.email}</td>
                  <td>{c.city}</td>
                  <td>{c.state}</td>
                  <td>
                    <span className={`badge ${c.loyalty_tier === "Gold" ? "badge-warning" : c.loyalty_tier === "Silver" ? "badge-neutral" : "badge-neutral"}`}>
                      {c.loyalty_tier}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
