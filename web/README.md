# Shop ML App (Supabase + Vercel)

This app lives in the `web/` subfolder and uses Supabase Postgres as its database.

## 1) Configure Supabase

Create a Supabase project, then copy your Postgres connection string into `.env.local`:

```bash
cp .env.example .env.local
```

Set:

```env
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@db.YOUR_PROJECT_REF.supabase.co:5432/postgres
```

## 2) Migrate existing SQLite data to Supabase

From `web/`:

```bash
npm install
npm run migrate:supabase
```

This script:
- creates the required tables in Supabase
- clears existing rows in those tables
- copies all rows from `../shop.db`

## 3) Run locally

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## 4) Deploy to Vercel

In Vercel:
- import the GitHub repository
- set **Root Directory** to `web`
- add environment variable `DATABASE_URL` (same value as local)
- deploy

## Notes

- The **Run Scoring** button now uses the deployed API route at `app/api/run-scoring/route.ts` and writes to `order_predictions`.
- No local SQLite file is needed in production.
