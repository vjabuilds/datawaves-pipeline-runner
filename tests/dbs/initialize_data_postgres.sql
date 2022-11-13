CREATE TABLE products (
	productId serial PRIMARY KEY,
	productName VARCHAR ( 50 ) NOT NULL,
	productCode VARCHAR ( 50 ) UNIQUE NOT NULL,
    productDescription VARCHAR ( 150 ) NULL,
	numberAvailable integer NOT NULL,
	price decimal NOT NULL,
    cost decimal NOT NULL
);

/*
    The price is the amount of money the store pays to get the item.
    The cost is the amount of money the store wants when it is selling.
*/

INSERT INTO products
    (productname, productcode, productdescription, numberavailable, price, "cost")
values
	('Boots', 'B-01', 'Red boots', 100, 150, 200),
	('Boots', 'B-02', 'Green boots', 95, 127, 200),
	('Bag', 'Bg-01', 'A handbag', 33, 16, 20),
	('Watch', 'W-01', 'A luxurious swiss watch', 1, 10000, 2000), /* someone messed up and left out a zero, Whoops*/
	('Walkie talkie', 'W-02', 'A high tech handheld walkie talkie', 10, 100, 200), 
	('Fedora', 'F-001', 'A super stylish fedora, very popular with people of all ages', 33, 50, 55),
	('Katana', 'K-556', 'A blade from the far east', 1, 150000, 450000);


CREATE TABLE orders (
	orderId serial PRIMARY KEY,
	orderCode VARCHAR ( 50 ) UNIQUE NOT NULL,
    orderDescription VARCHAR ( 150 ) NULL
);

CREATE TABLE order_products (
	orderProductsId serial PRIMARY KEY,
	orderId integer NOT NULL,
	productId integer not null,
	constraint order_products_order_fkey FOREIGN KEY(orderId) REFERENCES orders(orderId),
	CONSTRAINT order_products_products_fkey FOREIGN KEY(productId) REFERENCES products(productId)
);

INSERT INTO orders
    (orderCode, orderDescription)
values
	('O-01', 'An order for green boots'),
	('O-02', 'An order for different boots'),
	('O-03', 'An order for a katana and a fedora'),
	('O-04', 'An empty order :)');

insert into order_products 
	(orderId, productId)
values
	(
		(select orderId from orders where orderCode = 'O-01'),
		(select productId from products where productCode = 'B-02')
	),
	(
		(select orderId from orders where orderCode = 'O-02'),
		(select productId from products where productCode = 'B-01')
	),
	(
		(select orderId from orders where orderCode = 'O-02'),
		(select productId from products where productCode = 'B-02')
	),
	(
		(select orderId from orders where orderCode = 'O-03'),
		(select productId from products where productCode = 'K-556')
	),
	(
		(select orderId from orders where orderCode = 'O-03'),
		(select productId from products where productCode = 'F-001')
	);