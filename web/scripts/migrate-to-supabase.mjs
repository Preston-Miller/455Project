import path from "path";
import { fileURLToPath } from "url";
import Database from "better-sqlite3";
import pg from "pg";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL || process.env.SUPABASE_DB_URL;
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL (or SUPABASE_DB_URL) environment variable.");
  process.exit(1);
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const sqlitePath = path.join(__dirname, "..", "..", "shop.db");
const sqlite = new Database(sqlitePath, { readonly: true });
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const schemaSql = `
CREATE TABLE IF NOT EXISTS customers (
  customer_id      BIGINT PRIMARY KEY,
  full_name        TEXT NOT NULL,
  email            TEXT NOT NULL UNIQUE,
  gender           TEXT NOT NULL,
  birthdate        TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  city             TEXT,
  state            TEXT,
  zip_code         TEXT,
  customer_segment TEXT,
  loyalty_tier     TEXT,
  is_active        INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS products (
  product_id   BIGINT PRIMARY KEY,
  sku          TEXT NOT NULL UNIQUE,
  product_name TEXT NOT NULL,
  category     TEXT NOT NULL,
  price        DOUBLE PRECISION NOT NULL,
  cost         DOUBLE PRECISION NOT NULL,
  is_active    INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS orders (
  order_id        BIGINT PRIMARY KEY,
  customer_id     BIGINT NOT NULL REFERENCES customers(customer_id),
  order_datetime  TEXT NOT NULL,
  billing_zip     TEXT,
  shipping_zip    TEXT,
  shipping_state  TEXT,
  payment_method  TEXT NOT NULL,
  device_type     TEXT NOT NULL,
  ip_country      TEXT NOT NULL,
  promo_used      INTEGER NOT NULL DEFAULT 0,
  promo_code      TEXT,
  order_subtotal  DOUBLE PRECISION NOT NULL,
  shipping_fee    DOUBLE PRECISION NOT NULL,
  tax_amount      DOUBLE PRECISION NOT NULL,
  order_total     DOUBLE PRECISION NOT NULL,
  risk_score      DOUBLE PRECISION NOT NULL,
  is_fraud        INTEGER NOT NULL DEFAULT 0,
  fulfilled       INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_items (
  order_item_id BIGINT PRIMARY KEY,
  order_id      BIGINT NOT NULL REFERENCES orders(order_id),
  product_id    BIGINT NOT NULL REFERENCES products(product_id),
  quantity      INTEGER NOT NULL,
  unit_price    DOUBLE PRECISION NOT NULL,
  line_total    DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS shipments (
  shipment_id      BIGINT PRIMARY KEY,
  order_id         BIGINT NOT NULL UNIQUE REFERENCES orders(order_id),
  ship_datetime    TEXT NOT NULL,
  carrier          TEXT NOT NULL,
  shipping_method  TEXT NOT NULL,
  distance_band    TEXT NOT NULL,
  promised_days    INTEGER NOT NULL,
  actual_days      INTEGER NOT NULL,
  late_delivery    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS product_reviews (
  review_id        BIGINT PRIMARY KEY,
  customer_id      BIGINT NOT NULL REFERENCES customers(customer_id),
  product_id       BIGINT NOT NULL REFERENCES products(product_id),
  rating           INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
  review_datetime  TEXT NOT NULL,
  review_text      TEXT,
  UNIQUE(customer_id, product_id)
);

CREATE TABLE IF NOT EXISTS order_predictions (
  order_id                   BIGINT PRIMARY KEY REFERENCES orders(order_id),
  late_delivery_probability  DOUBLE PRECISION,
  predicted_late_delivery    INTEGER,
  prediction_timestamp       TEXT
);
`;

const tableConfigs = [
  {
    table: "customers",
    columns: [
      "customer_id", "full_name", "email", "gender", "birthdate", "created_at",
      "city", "state", "zip_code", "customer_segment", "loyalty_tier", "is_active",
    ],
    sequence: "customers_customer_id_seq",
    idColumn: "customer_id",
  },
  {
    table: "products",
    columns: ["product_id", "sku", "product_name", "category", "price", "cost", "is_active"],
    sequence: "products_product_id_seq",
    idColumn: "product_id",
  },
  {
    table: "orders",
    columns: [
      "order_id", "customer_id", "order_datetime", "billing_zip", "shipping_zip",
      "shipping_state", "payment_method", "device_type", "ip_country", "promo_used",
      "promo_code", "order_subtotal", "shipping_fee", "tax_amount", "order_total",
      "risk_score", "is_fraud", "fulfilled",
    ],
    sequence: "orders_order_id_seq",
    idColumn: "order_id",
  },
  {
    table: "order_items",
    columns: ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_total"],
    sequence: "order_items_order_item_id_seq",
    idColumn: "order_item_id",
  },
  {
    table: "shipments",
    columns: [
      "shipment_id", "order_id", "ship_datetime", "carrier", "shipping_method",
      "distance_band", "promised_days", "actual_days", "late_delivery",
    ],
    sequence: "shipments_shipment_id_seq",
    idColumn: "shipment_id",
  },
  {
    table: "product_reviews",
    columns: [
      "review_id", "customer_id", "product_id", "rating", "review_datetime", "review_text",
    ],
    sequence: "product_reviews_review_id_seq",
    idColumn: "review_id",
  },
  {
    table: "order_predictions",
    columns: [
      "order_id", "late_delivery_probability", "predicted_late_delivery", "prediction_timestamp",
    ],
  },
];

function copyRows(table, columns, client) {
  const rows = sqlite.prepare(`SELECT ${columns.join(", ")} FROM ${table}`).all();
  if (rows.length === 0) {
    console.log(`- ${table}: 0 rows`);
    return Promise.resolve();
  }

  const chunkSize = 250;
  const chunks = [];
  for (let i = 0; i < rows.length; i += chunkSize) {
    chunks.push(rows.slice(i, i + chunkSize));
  }

  return chunks
    .reduce((chain, chunk) => {
      return chain.then(async () => {
        const values = [];
        const valueGroups = chunk.map((row, rowIdx) => {
          const start = rowIdx * columns.length;
          values.push(...columns.map((col) => row[col]));
          const placeholders = columns.map((_, colIdx) => `$${start + colIdx + 1}`).join(", ");
          return `(${placeholders})`;
        });

        const insertSql = `INSERT INTO ${table} (${columns.join(", ")}) VALUES ${valueGroups.join(", ")}`;
        await client.query(insertSql, values);
      });
    }, Promise.resolve())
    .then(() => {
      console.log(`- ${table}: ${rows.length} rows`);
    });
}

async function setSequence(client, table, idColumn) {
  if (!idColumn) return;

  const { rows } = await client.query(
    "SELECT pg_get_serial_sequence($1, $2) AS sequence_name",
    [table, idColumn]
  );
  const sequence = rows[0]?.sequence_name;
  if (!sequence) return;

  await client.query(
    `SELECT setval($1::regclass, COALESCE((SELECT MAX(${idColumn}) FROM ${table}), 1), true)`,
    [sequence]
  );
}

async function ensureAutoId(client, table, idColumn) {
  const sequenceName = `${table}_${idColumn}_seq`;
  await client.query(`CREATE SEQUENCE IF NOT EXISTS ${sequenceName}`);
  await client.query(
    `ALTER TABLE ${table} ALTER COLUMN ${idColumn} SET DEFAULT nextval('${sequenceName}')`
  );
  await client.query(`ALTER SEQUENCE ${sequenceName} OWNED BY ${table}.${idColumn}`);
}

async function main() {
  const client = await pool.connect();
  try {
    console.log("Bootstrapping Supabase schema...");
    await client.query(schemaSql);
    await ensureAutoId(client, "orders", "order_id");
    await ensureAutoId(client, "order_items", "order_item_id");

    console.log("Clearing existing data...");
    await client.query(`
      TRUNCATE TABLE
        order_predictions,
        product_reviews,
        shipments,
        order_items,
        orders,
        products,
        customers
      RESTART IDENTITY CASCADE
    `);

    console.log("Copying data from SQLite...");
    for (const cfg of tableConfigs) {
      await copyRows(cfg.table, cfg.columns, client);
      await setSequence(client, cfg.table, cfg.idColumn);
    }

    console.log("Supabase migration complete.");
  } finally {
    client.release();
    await pool.end();
    sqlite.close();
  }
}

main().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
