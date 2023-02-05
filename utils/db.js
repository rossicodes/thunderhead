import pg from 'pg'
const { Pool } = pg

// pools will use environment variables
// for connection information

const pool = new Pool({
    host: '127.0.0.1',
    database: 'rossi',
    port: 5432,
    user: process.env.NEXT_PUBLIC_POSTGRES_USER,
    password: process.env.NEXT_PUBLIC_POSTGRES_PASS,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
})

export default {
    query: (text, params) => pool.query(text, params),
};