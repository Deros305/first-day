������ƣ�
1.ͳ��������Ů�����ѵ���Ʒ�������û���������
2.ͳ��ÿ�����ܻ�ӭ��ʮ����Ʒ���γ�����ǰʮ�񵥡�
3.ͳ��2022-6-14����Ь�Ĺ����û�,��ÿ����Ь�������Լ����ѵĽ�
4.ͳ��ÿ�չ���������������û����ǳƣ��������,���ѽ�
5.ͳ����������ǰ5�û��������˶��ٶ�ȡ�
6.ͳ����������ǰ6��ͬѧ����������Щ��ͬ����Ʒ

1������һ��ԭʼ��������Ӧԭʼ��json����
create table Tall(json string);
load data local inpath "/root/2022-06-14.log" into table Tall;
2.����json���� �����µı�
create table Tall_jiexi
as
select json_tuple(json,'product_category','product_create_date','product_id','product_name','product_price',
'product_sale_price','product_title','times','user_address','user_birthday','user_gender','user_homeplace',
'user_id','user_name','user_nickname','user_password') 
as(product_category,product_create_date,product_id,product_name,product_price,
product_sale_price,product_title,times,user_address,user_birthday,user_gender,user_homeplace,
user_id,user_name,user_nickname,user_password) from Tall;


������ƣ�
1.ͳ��������Ů�����û����������Ѻ���Ʒ������
select user_gender gender,sum(product_price) zonge,count(distinct user_id) use_counts from Tall_jiexi
group by user_gender; 
2.ͳ��ÿ�����ܻ�ӭ��ʮ����Ʒ���γ�����ǰʮ�񵥡�
select product_name,count(product_id) counts from Tall_jiexi
group by product_name
order by counts desc
limit 10;
3.ͳ��2022-6-14����Ь�Ĺ����û�,��ÿ����Ь�������Լ����ѵĽ�
select user_name name,count(product_name) sums,sum(product_price) zonge from Tall_jiexi
where times='2022-06-14' and product_name like "%Ь%"
group by user_name
order by zonge desc;
4.ͳ��ÿ�չ���������������û����ǳƣ��������,���ѽ�
select * from (select *,row_number()over(partition by a.dt order by a.counts desc) paiming
from
(select times dt,user_name,count(product_name) counts,sum(product_price) from Tall_jiexi
group by times,user_name)a)b
where b.paiming;

5.ͳ����������ǰ5�û��������˶��ٶ�ȡ�
select round(c.sums/b.sums2,2) from
(select sum(a.sums) sums from (select user_name,sum(product_price) sums from Tall_jiexi
group by user_name
order by sums desc
limit 5)a)c,
(select sum(product_price) as sums2 from Tall_jiexi)b;

6.ͳ����������ǰ6��ͬѧ����������Щ��ͬ����Ʒ
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

