CREATE TABLE posts (
  title VARCHAR(64),
  content TEXT,
  author VARCHAR(64),
  status VARCHAR(64),
  _deleted BOOL
);

INSERT INTO posts(title, content, author, status, _deleted)
VALUES
    ('title 1', 'What a great day', 'Jack', 'Private', false),
    ('title 2', 'What a great day', 'Jack', 'Draft', false),
    ('title 3', 'What a great day', 'Jack', 'Published', false),
    ('title 2', 'What a great day', NULL, 'Published', false);

    
CREATE TABLE orders (
    order_no VARCHAR(64),
    status VARCHAR(32),
    _deleted BOOL
);

INSERT INTO orders(order_no, status, _deleted)
VALUES
    ('PO20240101001', 'CREATED', false),
    ('PO20240101002', 'ACTIVE', false);
