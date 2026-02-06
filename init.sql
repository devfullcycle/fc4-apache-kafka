-- Schema e tabela de exemplo para CDC com Debezium

CREATE SCHEMA IF NOT EXISTS ecommerce;

CREATE TABLE ecommerce.orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_email VARCHAR(150) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE ecommerce.orders REPLICA IDENTITY FULL;

-- Dados de exemplo
INSERT INTO ecommerce.orders (customer_name, customer_email, product_name, quantity, unit_price, status) VALUES
('João Silva', 'joao.silva@email.com', 'Notebook Dell', 1, 4500.00, 'PENDING'),
('Maria Santos', 'maria.santos@email.com', 'Mouse Logitech', 2, 150.00, 'CONFIRMED'),
('Pedro Oliveira', 'pedro.oliveira@email.com', 'Teclado Mecânico', 1, 350.00, 'SHIPPED'),
('Ana Costa', 'ana.costa@email.com', 'Monitor 27"', 1, 1800.00, 'DELIVERED'),
('Carlos Souza', 'carlos.souza@email.com', 'Webcam HD', 3, 280.00, 'PENDING');

-- Função para atualizar updated_at automaticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger para atualizar updated_at
CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON ecommerce.orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
