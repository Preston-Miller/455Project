import { query } from "@/lib/db";

interface TableName {
  name: string;
}

interface ColumnInfo {
  cid: number;
  name: string;
  type: string;
  notnull: boolean;
  dflt_value: string | null;
  pk: boolean;
}

interface TableInfo {
  name: string;
  columns: ColumnInfo[];
  row_count: number;
}

function quoteIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Unsafe table name: ${value}`);
  }
  return `"${value}"`;
}

export default async function SchemaPage() {
  const tables = await query<TableName>(
    `SELECT table_name AS name
     FROM information_schema.tables
     WHERE table_schema = 'public'
       AND table_type = 'BASE TABLE'
     ORDER BY table_name`
  );

  const tableInfos: TableInfo[] = await Promise.all(tables.map(async (t) => {
    const columns = await query<ColumnInfo>(
      `SELECT
         cols.ordinal_position::int AS cid,
         cols.column_name AS name,
         cols.data_type AS type,
         (cols.is_nullable = 'NO') AS notnull,
         cols.column_default AS dflt_value,
         EXISTS (
           SELECT 1
           FROM information_schema.table_constraints tc
           JOIN information_schema.key_column_usage kcu
             ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          WHERE tc.table_schema = 'public'
            AND tc.table_name = cols.table_name
            AND tc.constraint_type = 'PRIMARY KEY'
            AND kcu.column_name = cols.column_name
         ) AS pk
       FROM information_schema.columns cols
       WHERE cols.table_schema = 'public'
         AND cols.table_name = $1
       ORDER BY cols.ordinal_position`,
      [t.name]
    );

    const countResult = await query<{ cnt: number }>(
      `SELECT COUNT(*)::int AS cnt FROM ${quoteIdentifier(t.name)}`
    );
    return {
      name: t.name,
      columns,
      row_count: countResult[0]?.cnt ?? 0,
    };
  }));

  return (
    <div>
      <div className="page-header">
        <h1>DB Schema</h1>
        <p>Live schema inspection of shop.db — {tables.length} tables</p>
      </div>

      {tableInfos.map((t) => (
        <div key={t.name} className="card">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "10px" }}>
            <div>
              <code style={{ fontSize: "16px", fontWeight: 700 }}>{t.name}</code>
            </div>
            <span className="badge badge-neutral">{t.row_count.toLocaleString()} rows</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>Column</th>
                  <th>Type</th>
                  <th>Not Null</th>
                  <th>Default</th>
                  <th>PK</th>
                </tr>
              </thead>
              <tbody>
                {t.columns.map((col) => (
                  <tr key={col.cid}>
                    <td style={{ color: "var(--muted)" }}>{col.cid}</td>
                    <td><strong>{col.name}</strong></td>
                    <td><code>{col.type || "—"}</code></td>
                    <td>{col.notnull ? "✓" : ""}</td>
                    <td style={{ color: "var(--muted)" }}>{col.dflt_value ?? "—"}</td>
                    <td>{col.pk ? "✓" : ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  );
}
