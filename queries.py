# -*- coding:utf-8 -*-

# updatenum всіх довідників, щоб вивантажити тільки зміни з попереднього циклу:
datapump_query = """select 'category' as name, max(updatenum) as updatenum
                      from grp
                    union all
                    select 'product', max(updatenum) from art
                    union all
                    select 'cashier', max(updatenum) from cashier
                    union all
                    select 'pack', max(updatenum) from pack
                 """
# [category;935 
#product;2765
#cashier;110
#pack;1963]
#935 - номер версії

sales_lastdate_query = """select shop_identifier
    , max(sales_dt)
  --, terminal_identifier
  --, receipt_identifier
  --, posnum
 from (select s.sareaid as shop_identifier
             , cast(s.sareaid as nvarchar) + '-'
               + cast(s.systemid as nvarchar)
               as terminal_identifier
             , s.salestime as sales_dt
             , s.srecnum as receipt_identifier
             , s.salesnum as posnum
             , sext.SALESEXTKEY
          from sales as s
     left join [DataServer_main].[dbo].[salesext] as sext
            on s.sareaid = sext.sareaid
           and s.SYSTEMID = sext.SYSTEMID
           and s.SESSID = sext.SESSID
           and s.SALESNUM = sext.SALESNUM
           and sext.DELFLAG = 0
           and sext.SALESEXTKEY in (15,16)
         where s.salestag = 0
           and s.salesrefund = 0
           and s.salesflags = 0
           and s.salescanc = 0
           and s.salessum <> 0
           and s.salescount <> 0
      ) as sales
where sales.SALESEXTKEY <> 16
  and not sales.terminal_identifier in ('12-2','3-2','1-3','9-2','9-3')
group by shop_identifier
"""
# [[shopid,datestano]]

unit_query = """select distinct cast(unitid as nvarchar)
                                + '-'
                                + cast(packquant as nvarchar)
                                + '-'
                                + cast(packdtype as nvarchar)
                                as unit_identifier
                     , case when packquant=1
                            then packname
                            else packname
                                 + ' ('+cast(packquant as nvarchar)+')'
                            end AS name
                     , packdtype AS packed
                     , packquant AS pack_capacity
                  from pack
                 where updatenum > %i and updatenum <= %i
             """
#[<old_version,new_version]

#terminal_query = """select distinct cast(sareaid as nvarchar)
#                                    + '-' + cast(systemid as nvarchar)
#                                    as terminal_identifier
#                                  , sareaid as shop_identifier
#                                  , 'Kassa '+cast(systemid as nvarchar) name
#                      from sales
#                 """


#shop_query = """select sareaid as shop_identifier, sareaname as name
#                  from sarea where sareaid not in (0,11)"""

cashier_query = """select cashierid as cashier_identifier, cashiername as name
                     from cashier
                    where cashiergrpid in (1,4)
                     and updatenum > %i and updatenum <= %i
               """
#for new cachiers [[cachierid,name]]

category_query = """select grpid as category_identifier
                        , parentgrpid as category_parent_identifier
                        , grpname as name
                     from grp
                    where updatenum > %i and updatenum <= %i
                """
#for new categories [[cat_id, parent_id, cat_name]]

product_query = """select cast(a.artid as nvarchar)
                          + '-'
                          + cast(p.packid as nvarchar) AS product_identifier
                        , grpid category_identifier
                        , cast(unitid as nvarchar)
                          + '-'
                          + cast(packquant as nvarchar)
                          + '-'
                          + cast(packdtype as nvarchar) AS unit_identifier
                        , artname + case when p.unitid=1 and packquant=1
                                         then ''
                                         else ' ('+case when packquant=1
                                                        then packname
                                                        else packname
                                                             + ' ('
                                                             + cast(packquant as nvarchar)
                                                             + ')'
                                                   end + ')'
                                    end AS name
                     from art a
                     join pack p
                       on a.artid = p.artid
                    where (a.updatenum > %i and a.updatenum <= %i)
                       or (p.updatenum > %i and p.updatenum <= %i)
                """
#for new product [[prod_id,  cat_id, prod_name]]

receipt_query = """select sales_dt
                        , terminal_identifier
                        , receipt_identifier
                        , posnum
                        , price
                        , total_price
                        , qty
                        , product_identifier
                        , packed
                        , cashier_identifier
                   from (select -- s.sareaid as shop_identifier
                                cast(s.sareaid as nvarchar) + '-' +
                                cast(s.systemid as nvarchar) as terminal_identifier
                              , s.salestime as sales_dt
                              , s.srecnum as receipt_identifier
                              , s.salesnum as posnum
                              , cast(s.salesprice/100 as decimal(20,2)) as price
                              , cast(s.salessum/100 as decimal(20,2)) as total_price
                              , case when s.salestype=0
                                     then cast(s.salescount as decimal)/1000
                                     else s.salescount
                                end as qty
                              , cast(s.salescode as nvarchar) + '-' +
                                s.packname as product_identifier
                              , s.salestype as packed
                              , s.cashierid as cashier_identifier
                              , sext.SALESEXTKEY
                           from sales as s
                      left join [DataServer_main].[dbo].[salesext] as sext
                             on s.sareaid = sext.sareaid
                            and s.SYSTEMID = sext.SYSTEMID
                            and s.SESSID = sext.SESSID
                            and s.SALESNUM = sext.SALESNUM
                            and sext.DELFLAG = 0
                            and sext.SALESEXTKEY in (15,16)
                          where s.salestag = 0
                            and s.salesrefund = 0
                            and s.salesflags = 0
                            and s.salescanc = 0
                            and s.salesprice <> 0
                            and s.salessum <> 0
                            and s.salescount <> 0
                            and s.sareaid = %(shop_id)s
                            and s.salestime >= '%(date_from)s'
                            and s.salestime < '%(date_to)s'
                        ) as sales
                   where sales.SALESEXTKEY <> 16
                     and not sales.terminal_identifier in
                         ('12-2','3-2','1-3','9-2','9-3')
                   order by 1,2,3,4
                """
#[[sales_dt, terminal_identifier, receipt_identifier, posnum, price, total_price, qty, product_identifier, packed, cashier_identifier],
#[sales_dt, terminal_identifier, receipt_identifier, posnum, price, total_price, qty, product_identifier, packed, cashier_identifier]]
