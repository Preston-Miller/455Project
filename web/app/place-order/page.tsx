import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { query } from "@/lib/db";
import PlaceOrderForm from "./PlaceOrderForm";

interface Product {
  product_id: number;
  product_name: string;
  category: string;
  price: number;
}

export default async function PlaceOrderPage() {
  const cookieStore = await cookies();
  const customerId = cookieStore.get("customer_id")?.value;
  if (!customerId) redirect("/select-customer");

  const products = await query<Product>(
    "SELECT product_id, product_name, category, price FROM products WHERE is_active = 1 ORDER BY category, product_name"
  );

  return (
    <div>
      <div className="page-header">
        <h1>Place Order</h1>
        <p>Select products and quantities. The order will be saved to the database.</p>
      </div>
      <PlaceOrderForm products={products} />
    </div>
  );
}
