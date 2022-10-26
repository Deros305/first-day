需求设计：
1.统计男生和女生消费的商品总量和用户总数量。
2.统计每日最受欢迎的十个商品，形成热卖前十榜单。
3.统计2022-6-14购买鞋的购买用户,和每人买鞋的数量以及消费的金额。
4.统计每日购买次数最多的三个用户的昵称，购买次数,消费金额。
5.统计消费排名前5用户，贡献了多少额度。
6.统计消费排名前6的同学都购买了哪些相同的商品

1、创建一个原始表用来对应原始的json数据
create table Tall(json string);
load data local inpath "/root/2022-06-14.log" into table Tall;
2.解析json数据 创建新的表
create table Tall_jiexi
as
select json_tuple(json,'product_category','product_create_date','product_id','product_name','product_price',
'product_sale_price','product_title','times','user_address','user_birthday','user_gender','user_homeplace',
'user_id','user_name','user_nickname','user_password') 
as(product_category,product_create_date,product_id,product_name,product_price,
product_sale_price,product_title,times,user_address,user_birthday,user_gender,user_homeplace,
user_id,user_name,user_nickname,user_password) from Tall;


需求设计：
1.统计男生和女生的用户总数量消费和商品总量。
select user_gender gender,sum(product_price) zonge,count(distinct user_id) use_counts from Tall_jiexi
group by user_gender; 
2.统计每日最受欢迎的十个商品，形成热卖前十榜单。
select product_name,count(product_id) counts from Tall_jiexi
group by product_name
order by counts desc
limit 10;
3.统计2022-6-14购买鞋的购买用户,和每人买鞋的数量以及消费的金额。
select user_name name,count(product_name) sums,sum(product_price) zonge from Tall_jiexi
where times='2022-06-14' and product_name like "%鞋%"
group by user_name
order by zonge desc;
4.统计每日购买次数最多的三个用户的昵称，购买次数,消费金额。
select * from (select *,row_number()over(partition by a.dt order by a.counts desc) paiming
from
(select times dt,user_name,count(product_name) counts,sum(product_price) from Tall_jiexi
group by times,user_name)a)b
where b.paiming;

5.统计消费排名前5用户，贡献了多少额度。
select round(c.sums/b.sums2,2) from
(select sum(a.sums) sums from (select user_name,sum(product_price) sums from Tall_jiexi
group by user_name
order by sums desc
limit 5)a)c,
(select sum(product_price) as sums2 from Tall_jiexi)b;

6.统计消费排名前6的同学都购买了哪些相同的商品
select * from (select c.btitle,count(*)counts from
(select b.user_name bname,b.product_name btitle from
(select user_name,sum(product_price) zonge from Tall_jiexi
group by user_name order by zonge desc
limit 6)a
join Tall_jiexi b
on a.user_name=b.user_name
group by b.user_name,b.product_name)c
group by c.btitle)d
where d.counts=6;

