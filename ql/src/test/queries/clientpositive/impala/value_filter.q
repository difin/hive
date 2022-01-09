--! qt:dataset:impala_dataset

explain cbo select tmp.str from (values
('testfield' as str),
('hello'),
('world')) as tmp
where tmp.str = 'hello';

explain cbo physical select tmp.str from (values
('testfield' as str),
('hello'),
('world')) as tmp
where tmp.str = 'hello';
