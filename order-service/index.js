import express from 'express';
import axios from 'axios';
import db from './db.js';
import { connectToBroker, publishMessage } from './broker.js';

const app = express();
app.use(express.json());

// RabbitMQ
connectToBroker().catch(err => console.error('Broker init error', err));

// Create order
app.post('/', async (req, res) => {
  try {
    const { productId, quantity } = req.body;

    // 1. Validate request body
    if (!productId || !quantity || Number(quantity) <= 0) {
      return res.status(400).json({ error: 'Valid productId and positive quantity are required' });
    }

    // 2. Call product service to verify product exists
    const productServiceUrl = process.env.PRODUCT_SERVICE_URL || 'http://product-service:8002';
    let product;
    try {
      const response = await axios.get(`${productServiceUrl}/${productId}`, { timeout: 5000 });
      product = response.data;
    } catch (err) {
      if (err.response && err.response.status === 404) {
        return res.status(404).json({ error: 'Product not found' });
      }
      console.error('Error fetching product:', err.message);
      return res.status(502).json({ error: 'Failed to verify product with product-service' });
    }

    // 3. Insert order into database
    const r = await db.query(
      'INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, $3) RETURNING *',
      [productId, Number(quantity), 'PENDING']
    );
    const order = r.rows[0];

    // 4. Publish order.created event to message broker
    const eventPayload = {
      event: 'ORDER_CREATED',
      orderId: order.id,
      product,
      quantity: order.quantity,
      status: order.status,
      createdAt: order.created_at
    };
    await publishMessage('orders', eventPayload);
    console.log('Published ORDER_CREATED event to RabbitMQ:', JSON.stringify(eventPayload));

    // 5. Return success response with order details
    res.status(201).json(order);
  } catch (err) {
    console.error('Create order error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// List orders
app.get('/', async (_req, res) => {
  const r = await db.query('SELECT * FROM orders ORDER BY id DESC');
  res.json(r.rows);
});

// Get order by id
app.get('/:id', async (req, res) => {
  const id = Number(req.params.id);
  const r = await db.query('SELECT * FROM orders WHERE id = $1', [id]);
  if (r.rows.length === 0) return res.status(404).json({ error: 'Order not found' });
  res.json(r.rows[0]);
});

const PORT = 8003;
app.listen(PORT, () => console.log(`Order Service running on ${PORT}`));