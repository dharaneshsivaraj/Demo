create or replace package body xx_ar_iface_transactions
as
 
  procedure XX_STG_TBL_PROCESS
(
p_IC_TYPE			IN VARCHAR2,
p_SOURCE_ENTITY              IN   VARCHAR2,
p_SOURCE_CUSTOMER_CODE           IN   VARCHAR2,
p_SOURCE_ENTITY_NUMBER              IN   VARCHAR2,
p_TRANSFERED_ENTITY                 IN   VARCHAR2,
p_TRANSFERED_CUSTOMER_CODE             IN   VARCHAR2,
p_TRANSFERED_CUSTOMER_NAME          IN   VARCHAR2,
p_TRANSFERED_ENTITY_NUMBER         IN   VARCHAR2,
p_TRANSFERED_ENTITY_DATE           IN   VARCHAR2,
p_TRANSACTION_TYPE             IN   VARCHAR2,
p_CURRENCY in VARCHAR2,
p_APPLY_AMOUNT                  IN   number,
P_BATCH_NUMBER   in VARCHAR2,
p_userid in VARCHAR2 
)
as
begin




insert into  xx_ar_transactions_stg
(IC_TYPE
,SOURCE_ENTITY
,SOURCE_CUSTOMER_CODE
,SOURCE_ENTITY_NUMBER
,TRANSFERED_ENTITY
,TRANSFERED_CUSTOMER_CODE
,TRANSFERED_CUSTOMER_NAME
,TRANSFERED_ENTITY_NUMBER
,TRANSFERED_ENTITY_DATE
,TRANSACTION_TYPE
,CURRENCY
,APPLY_AMOUNT
,BATCH_NUMBER
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,CREATED_BY
,CREATION_DATE
)
values
(
p_IC_TYPE					,
p_SOURCE_ENTITY             ,
p_SOURCE_CUSTOMER_CODE      ,
p_SOURCE_ENTITY_NUMBER      ,
p_TRANSFERED_ENTITY         ,
p_TRANSFERED_CUSTOMER_CODE  ,
p_TRANSFERED_CUSTOMER_NAME  ,
p_TRANSFERED_ENTITY_NUMBER  ,
p_TRANSFERED_ENTITY_DATE    ,
p_TRANSACTION_TYPE          ,
p_CURRENCY 					,
p_APPLY_AMOUNT              ,
P_BATCH_NUMBER,
sysdate ,
p_userid,
p_userid,
p_userid,
sysdate
);

commit;			
Exception when others then
rollback;	
end  XX_STG_TBL_PROCESS;




procedure get_batch_number(p_batch_number out number)
as
l_batch_no number;
begin

select  xxsify_trans_batchno.nextval  into l_batch_no  from dual ;

p_batch_number := l_batch_no;
exception when others then
l_batch_no := null;
p_batch_number := l_batch_no;

end  get_batch_number;



procedure delete_batch (p_batch_number in varchar2)
as

begin 


delete from xx_ar_transactions_stg where  batch_number =  p_batch_number;

commit;


end delete_batch;

procedure rt_validations(p_batch_number in varchar2)
as
cursor rt
is
SELECT
rowid,
    ic_type,
    source_entity,
    source_customer_code,
    source_entity_number,
    transfered_entity,
    transfered_customer_code,
    transfered_customer_name,
    transfered_entity_number,
    transfered_entity_date,
    transaction_type,
    currency,
    apply_amount,
    batch_number,
    status_flag,
    error_msg
FROM
    xx_ar_transactions_stg
WHERE
    batch_number =  p_batch_number
    and nvl(status_flag , 'N') = 'N';

l_SOURCE_ENTITY_id  number;
l_status varchar2(20);
l_error_desc  varchar2(2000);
l_CASH_RECEIPT_ID  number;
l_TRANSFERED_ENTITY_ID number;
l_src_CUST_ACCOUNT_ID number;
l_tns_cust_acc_id   number;
l_CUSTOMER_TRX_ID  number;
l_trx_type_id number;
l_currency_code varchar2(200);
l_receipt_amount number;
l_stg_invoice_amt number;
l_inv_amt  number;
l_status_desc  varchar2(200);
l_INVOICE_CURRENCY_CODE varchar2(200);
l_inv_trx_type_id  number;
l_date  date;
l_closing_status varchar2(200);
l_conv_rate_cnt  number;
l_rec_CURRENCY_CODE  varchar2(200);
l_stg_inv_amt   number;
begin

for i in rt
loop
l_error_desc:= null;
l_status:= 'V';
l_SOURCE_ENTITY_id:= null;
l_CASH_RECEIPT_ID := null;
l_TRANSFERED_ENTITY_ID :=  null;
l_src_CUST_ACCOUNT_ID := null;
l_tns_cust_acc_id := null;
l_CUSTOMER_TRX_ID  := null;
l_trx_type_id :=  null;
l_currency_code := null;
l_receipt_amount := null;
l_stg_invoice_amt:= null;
l_inv_amt:= null;
l_status_desc:=  'Validation Success';
l_INVOICE_CURRENCY_CODE :=  null;
l_inv_trx_type_id := null;
l_date :=  null;
l_closing_status:= null;
l_conv_rate_cnt := null;
l_rec_CURRENCY_CODE := null;
l_stg_inv_amt  := null;
--- source entity
begin
select  ORGANIZATION_ID into l_SOURCE_ENTITY_id from hr_operating_units where SHORT_CODE = i.SOURCE_ENTITY;

exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid SOURCE_ENTITY';
l_status_desc:= 'Validation Error';
end;


---SOURCE_CUSTOMER_CODE
begin

 SELECT CUST_ACCOUNT_ID into l_src_CUST_ACCOUNT_ID    FROM HZ_CUST_ACCOUNTS HCA
   WHERE  hca.account_number = i.SOURCE_CUSTOMER_CODE;

exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid SOURCE_CUSTOMER_CODE';
l_status_desc:= 'Validation Error';
end;


--SOURCE_ENTITY_NUMBER
begin
SELECT CASH_RECEIPT_ID , CURRENCY_CODE
      INTO l_CASH_RECEIPT_ID , l_rec_CURRENCY_CODE
      FROM ar_cash_receipts_all
      WHERE receipt_number = i.SOURCE_ENTITY_NUMBER 
      and org_id  = l_SOURCE_ENTITY_id;
exception when others then
       l_status := 'Y' ;
     l_error_desc :=l_error_desc ||' Invalid SOURCE_ENTITY_NUMBER';
     l_status_desc:= 'Validation Error';
end;


--TRANSFERED_ENTITY
begin
select  ORGANIZATION_ID into l_TRANSFERED_ENTITY_ID from hr_operating_units where SHORT_CODE = i.TRANSFERED_ENTITY;
exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid TRANSFERED_ENTITY';
l_status_desc:= 'Validation Error';
end;


--TRANSFERED_CUSTOMER_CODE, TRANSFERED_CUSTOMER_NAME
Begin
SELECT distinct CUST_ACCOUNT_ID into l_tns_cust_acc_id 
    FROM HZ_PARTIES HP,
      HZ_CUST_ACCOUNTS HCA
    WHERE HP.PARTY_ID = HCA.PARTY_ID
--    AND HP.PARTY_NAME = i.TRANSFERED_CUSTOMER_NAME
    and hca.account_number = i.TRANSFERED_CUSTOMER_CODE;

exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid TRANSFERED_CUSTOMER_CODE, TRANSFERED_CUSTOMER_NAME ';
l_status_desc:= 'Validation Error';
end;


begin

--   SELECT  CUSTOMER_TRX_ID  into l_CUSTOMER_TRX_ID
--                 FROM ra_customer_trx_all
--                WHERE trx_number = i.TRANSFERED_ENTITY_NUMBER AND org_id=l_TRANSFERED_ENTITY_ID;


   SELECT    rcta.CUSTOMER_TRX_ID, nvl(AMOUNT_DUE_REMAINING,0) , rcta.INVOICE_CURRENCY_CODE  ,rcta.CUST_TRX_TYPE_ID
   into l_CUSTOMER_TRX_ID,l_inv_amt , l_INVOICE_CURRENCY_CODE , l_inv_trx_type_id
                 FROM ra_customer_trx_all rcta ,
                 ar_payment_schedules_all ps
                WHERE 
                ps.CUSTOMER_TRX_ID = rcta.CUSTOMER_TRX_ID
                and rcta.trx_number = i.TRANSFERED_ENTITY_NUMBER
                and rcta.org_id = l_TRANSFERED_ENTITY_ID;



exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid TRANSFERED_ENTITY_NUMBER' ;
l_status_desc:= 'Validation Error';
end;

dbms_output.put_line(l_inv_amt || i.APPLY_AMOUNT );
--if l_inv_amt < i.APPLY_AMOUNT then
--l_status:= 'E';
--l_error_desc :=l_error_desc || ' APPLY AMOUNT should be less than or equal to invoice amount  ' ;
--l_status_desc:= 'Validation Error';
--end if;


---- invoice amount check 

select sum(APPLY_AMOUNT) into l_stg_inv_amt from XX_AR_TRANSACTIONS_STG
where BATCH_NUMBER = i.BATCH_NUMBER 
and TRANSFERED_ENTITY_NUMBER = i.TRANSFERED_ENTITY_NUMBER;



dbms_output.put_line(l_inv_amt || i.APPLY_AMOUNT );
if l_inv_amt < l_stg_inv_amt then
l_status:= 'E';
l_error_desc :=l_error_desc || ' APPLY AMOUNT should be less than or equal to invoice amount  ' ;
l_status_desc:= 'Validation Error';
end if;








if i.APPLY_AMOUNT  <= 0  then
l_status:= 'E';
l_error_desc :=l_error_desc || ' APPLY AMOUNT should be greater than 0  ' ;
l_status_desc:= 'Validation Error';
end if;



begin

SELECT cust_trx_type_id    into l_trx_type_id              
                 FROM ra_cust_trx_types_all
                WHERE name = i.TRANSACTION_TYPE;
                

if l_inv_trx_type_id = l_trx_type_id then
null;
else
l_status:= 'E';
l_error_desc :=l_error_desc || ' Given  Transaction Type differs from Transaction Type of Transfered Entity ' ;
l_status_desc:= 'Validation Error';

end if;
                

exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid TRANSACTION_TYPE' ;
l_status_desc:= 'Validation Error';
end ;


Begin
 SELECT currency_code
              INTO l_currency_code
              FROM fnd_currencies
              WHERE currency_code =  i.CURRENCY;
              
if l_INVOICE_CURRENCY_CODE =  i.CURRENCY then
null;
else

l_status:= 'E';
l_error_desc :=l_error_desc || ' Given  currency code differs from currency code of Transfered Entity Number ' ;
l_status_desc:= 'Validation Error';
end if ;



if l_rec_CURRENCY_CODE =  i.CURRENCY then
null;
else

l_status:= 'E';
l_error_desc :=l_error_desc || ' Given  currency code differs from currency code of Source Entity Number ' ;
l_status_desc:= 'Validation Error';
end if ;











 
exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid CURRENCY ' ;
l_status_desc:= 'Validation Error';
end ;


 Select   SUM(b.AMOUNT_APPLIED) into l_receipt_amount 
from apps. AR_CASH_RECEIPTS_ALL a,
apps. AR_RECEIVABLE_APPLICATIONS_ALL b
WHERE  
  a.cash_receipt_id=b.cash_receipt_id
AND b.STATUS='UNAPP'
and receipt_number  =i.SOURCE_ENTITY_NUMBER
and a.org_id  = l_SOURCE_ENTITY_id;


select sum(APPLY_AMOUNT) into l_stg_invoice_amt from XX_AR_TRANSACTIONS_STG
where BATCH_NUMBER = i.BATCH_NUMBER 
and SOURCE_ENTITY_NUMBER = i.SOURCE_ENTITY_NUMBER;

 


if l_receipt_amount >= l_stg_invoice_amt then
null;

else
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invoice amt does not match the given receipts ' ;

l_status_desc:= 'Validation Error';
end if;

--- conversion rate 


SELECT
    closing_status into l_closing_status 
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id =l_SOURCE_ENTITY_id )
AND SYSDATE BETWEEN start_date AND end_date;




if l_closing_status = 'O'  then

l_date := sysdate;
else
begin
SELECT
   max(END_DATE)  into l_date
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id = l_SOURCE_ENTITY_id)
and closing_status = 'O';

exception when others then
l_date := sysdate;
end;

end if; 

if i.currency  = 'INR' 
then

null;

else


begin

select count(1) into l_conv_rate_cnt   from 
gl_daily_rates
where 
FROM_CURRENCY= i.currency
and TO_CURRENCY = 'INR'
 and to_date(CONVERSION_DATE) = to_date(l_date);


end;


end if;



if l_conv_rate_cnt = 0 then

l_status:= 'E';
l_error_desc :=l_error_desc || ' Currency rate has not yet defined for  '||i.currency ;

l_status_desc:= 'Validation Error';


end if;


 --- transfered_entity and source_entity
 
if i.transfered_entity = i.source_entity then

l_status:= 'E';
l_error_desc :=l_error_desc || ' transfered_entity and source_entity should not be same' ;

l_status_desc:= 'Validation Error';


end if;

 




update xx_ar_transactions_stg 
set 
status_flag = l_status,
status_desc = l_status_desc,
error_msg = l_error_desc,
source_OU_ID = l_SOURCE_ENTITY_id,
SOURCE_CUST_ACC_ID=l_src_CUST_ACCOUNT_ID
,SOURCE_ENTITY_ID=l_CASH_RECEIPT_ID
, TRANSFERED_OU_ID  = l_TRANSFERED_ENTITY_ID
, TRANSFERED_CUST_ACC_ID=l_tns_cust_acc_id
, TRANSFERED_ENTITY_ID= l_CUSTOMER_TRX_ID
, CUST_TRX_TYPE_ID=l_trx_type_id
where rowid = i.rowid;

end loop;
commit;



--- for consolidating the errors to receipt number
for j in (  
    select distinct SOURCE_ENTITY, SOURCE_CUSTOMER_CODE, SOURCE_ENTITY_NUMBER , ERROR_MSG ,STATUS_FLAG from 
    xx_ar_transactions_stg
    where batch_number = p_batch_number
    and STATUS_FLAG = 'E' )
    loop
    
   update xx_ar_transactions_stg 
set 
status_flag = j.STATUS_FLAG,
status_desc = 'Validation Error'
--error_msg = j.ERROR_MSG
where
batch_number = p_batch_number
and     SOURCE_ENTITY = j.SOURCE_ENTITY
    and SOURCE_ENTITY_NUMBER  = j.SOURCE_ENTITY_NUMBER ;
    
    
    
    end loop;


commit;
end rt_validations;


procedure DN_creation_API(p_batch_number in varchar2)
as
cursor c1 is

SELECT
   sum(apply_amount)  apply_amount,
    batch_number,
    ic_type,
    source_entity,
    source_customer_code,
--    source_entity_number,
    transfered_entity,
--    source_entity_id,
    source_cust_acc_id,
    source_ou_id
    ,CURRENCY
--    TRANSFERED_ENTITY_NUMBER
--source_entity_number
FROM
    xx_ar_transactions_stg
WHERE
    batch_number = p_batch_number
    AND status_flag = 'V'  
    group by     batch_number,
    ic_type,
    source_entity,
    source_customer_code,
--    source_entity_number,
    transfered_entity,
--    source_entity_id,
    source_cust_acc_id,
    source_ou_id ,
    CURRENCY;

 v_memo_line_id          NUMBER          DEFAULT NULL;
v_trx_header_id         NUMBER          DEFAULT NULL;
l_customer_trx_id       NUMBER;
o_return_status         VARCHAR2(1);
o_msg_count             NUMBER;
o_msg_data              VARCHAR2(2000);
 l_cnt                   NUMBER          DEFAULT 0;
l_msg_index_out         NUMBER;
l_trx_number            VARCHAR2(100);
l_status varchar2(20);
l_error_desc  varchar2(2000);
v_batch_source_id       ra_batch_sources_all.batch_source_id%TYPE DEFAULT NULL;
l_batch_source_rec      ar_invoice_api_pub.batch_source_rec_type;
l_memo_line_name  varchar2(200);
 l_cust_trx_type_id    ra_customer_trx_all.CUST_TRX_TYPE_ID%TYPE DEFAULT NULL;
l_trx_header_tbl        ar_invoice_api_pub.trx_header_tbl_type;
l_trx_lines_tbl         ar_invoice_api_pub.trx_line_tbl_type;
l_trx_dist_tbl          ar_invoice_api_pub.trx_dist_tbl_type;
l_trx_salescredits_tbl  ar_invoice_api_pub.trx_salescredits_tbl_type;
l_status_desc varchar2(200);
l_closing_status  varchar2(200);
l_date date;
l_responsiblity_id number;
l_exchange_rate   number;
l_ct_ref   varchar2(200);
begin

for i in c1 
loop

fnd_file.put_line(fnd_file.LOG,  '  loop 1 '); 


l_status :='DNS';
l_error_desc  := null;
l_status_desc:= 'Success';
l_closing_status :=  null;
l_date := null;
l_responsiblity_id := null;
l_exchange_rate := null;
l_ct_ref :=  null;

   BEGIN
fnd_file.put_line(fnd_file.LOG,  '  loop 2 '); 

        SELECT ra_customer_trx_s.nextval 
          INTO v_trx_header_id 
          FROM dual;

        EXCEPTION WHEN OTHERS THEN
fnd_file.put_line(fnd_file.LOG,  '  error while getting the ra_customer_trx_s sequence '); 

        l_status := 'DNE';
        l_error_desc :=l_error_desc || ' error while getting the ra_customer_trx_s sequence ' ;
        l_status_desc:= 'Debit Note Error';
        END ;

begin
fnd_file.put_line(fnd_file.LOG,  '  loop 3 '); 

select BATCH_SOURCE_ID into v_batch_source_id
from ra_batch_sources_all
where upper(name) like upper('IC-RT')
and org_id =i.SOURCE_OU_ID ;
exception when others then
fnd_file.put_line(fnd_file.LOG,  ' error while getting the batch source '); 
 l_status_desc:= 'Debit Note Error';
l_status:= 'DNE';
l_error_desc :=l_error_desc || ' error while getting the batch source ' ;
end;



--v_batch_source_id :=13003;


begin
fnd_file.put_line(fnd_file.LOG,  '  loop 4 '); 

select MEANING into l_memo_line_name 
from fnd_lookup_values 
where lookup_type = 'XX_AR_TRANS_MEMO_LINE' 
and DESCRIPTION = i.source_entity
and tag = i.TRANSFERED_ENTITY;


SELECT    t.memo_line_id into v_memo_line_id
  FROM ar_memo_lines_all_tl t
      ,ar_memo_lines_all_b b
 WHERE  b.memo_line_id = t.memo_line_id
   AND  t.name =   l_memo_line_name
   and b.org_id  =i.source_ou_id ;

exception when others then

fnd_file.put_line(fnd_file.LOG,  '  error while getting the  MEMO LINE NAME'); 
 l_status_desc:= 'Debit Note Error';
l_status:= 'DNE';
l_error_desc :=l_error_desc || ' error while getting the  MEMO LINE NAME ' ;

end;





begin
fnd_file.put_line(fnd_file.LOG,  '  loop 5 '); 

 select rctta.CUST_TRX_TYPE_ID  into l_cust_trx_type_id from 
      gl_code_combinations_kfv gcc, ra_cust_trx_types_all rctta
 where
rctta.GL_ID_REV =  gcc.code_combination_id and   rctta.type ='DM'
and gcc.segment2  in (
 select gcc.segment2  from ar_cash_receipts_all rcra ,
                AR_RECEIPT_METHOD_ACCOUNTS_ALL rmaa,
                gl_code_combinations_kfv gcc
                where 
                rcra.RECEIPT_METHOD_ID = rmaa.RECEIPT_METHOD_ID
--                and receipt_number =( select source_entity_number from 
--                                            xx_ar_transactions_stg
--                                        WHERE
--                                            batch_number = p_batch_number
--                                            AND status_flag = 'V' 
--                                            and  source_customer_code = i.source_customer_code and rownum= 1) 
                and rcra.org_id = i.source_ou_id
                and rmaa.CASH_CCID = gcc.code_combination_id
                and rownum= 1
              
) and rownum = 1   and rctta.org_id= i.source_ou_id   and  rctta.NAME  like  '%-DM';

exception when others 
then

fnd_file.put_line(fnd_file.LOG,  '  Error in getting trx type '); 
  l_status_desc:= 'Debit Note Error';
l_status:= 'DNE';
l_error_desc :=l_error_desc || ' error while getting the  trx type ' ;

--l_cust_trx_type_id:= 17004;
dbms_output.put_line('l_cust_trx_type_id   '||l_cust_trx_type_id);

end ;

fnd_file.put_line(fnd_file.LOG,  '  while getting the  l_closing_status '); 



SELECT
    closing_status into l_closing_status 
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id =i.SOURCE_OU_ID )
AND SYSDATE BETWEEN start_date AND end_date;




if l_closing_status = 'O'  then

l_date := sysdate;
else
begin
SELECT
   max(END_DATE)  into l_date
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id = i.SOURCE_OU_ID)
and closing_status = 'O';

exception when others then
l_date := sysdate;
end;

end if; 

fnd_file.put_line(fnd_file.LOG,  '  while getting the  l_date '); 



if i.CURRENCY = 'INR' then
fnd_file.put_line(fnd_file.LOG,  '  while getting the INR CURRENCY '||i.CURRENCY); 
 l_trx_header_tbl (1).exchange_rate_type := null;  --Corporate
   l_trx_header_tbl (1).trx_currency           := null;
null;
else

fnd_file.put_line(fnd_file.LOG,  '  while getting the  CURRENCY '||i.CURRENCY); 

 l_trx_header_tbl (1).exchange_rate_type := 'Corporate';  --Corporate
   l_trx_header_tbl (1).trx_currency           := i.CURRENCY;
 begin
 
  fnd_file.put_line(fnd_file.LOG,  '    in getting l_date '||l_date); 

-- 
--   select CONVERSION_RATE into l_exchange_rate  from 
--gl_daily_rates
--where 
--FROM_CURRENCY= i.currency
--and TO_CURRENCY = 'INR'
-- and to_date(CONVERSION_DATE) = TO_DATE(l_date);
-- fnd_file.put_line(fnd_file.LOG,  '    in getting l_exchange_rate '||l_exchange_rate); 
--
-- l_trx_header_tbl (1).exchange_rate := l_exchange_rate;
 
 
 
 exception when others then
 
 
fnd_file.put_line(fnd_file.LOG,  '  Error in getting CONVERSION_RATE '||sqlerrm); 
  l_status_desc:= 'Debit Note Error';
l_status:= 'DNE';
l_error_desc :=l_error_desc || ' error while getting the  CONVERSION_RATE ' ;

 
 end;
 
 

end if;


begin

select  source_entity_number into l_ct_ref
FROM
    xx_ar_transactions_stg
WHERE
    batch_number = p_batch_number
    AND SOURCE_CUST_ACC_ID =  i.SOURCE_CUST_ACC_ID
    and rownum =1 ;

exception when others then

l_ct_ref :=  null;

end;




            l_trx_header_tbl (1).trx_header_id                  := v_trx_header_id;
            l_trx_header_tbl (1).trx_number                     := NULL;
            l_trx_header_tbl (1).bill_to_customer_id            := i.SOURCE_CUST_ACC_ID;
            l_trx_header_tbl (1).cust_trx_type_id               := l_cust_trx_type_id;
              l_trx_header_tbl (1).trx_date               := l_date;
                l_trx_header_tbl (1).gl_date               := l_date;
            l_trx_header_tbl (1).comments                       := 'Intercompany transfer from '||i.source_entity ||' to '||i.transfered_entity     ;
             l_trx_header_tbl (1).interface_header_attribute1    :='RT-'||i.source_entity ||'-'||i.transfered_entity    ;
           l_trx_header_tbl (1).reference_number :='RT-'||i.source_entity ||'-'||i.transfered_entity    ;
            l_batch_source_rec.batch_source_id                  := v_batch_source_id;
              l_trx_header_tbl (1).interface_header_attribute15   := 'TN - CHENNAI:Bill To';
            l_trx_lines_tbl (1).trx_header_id                   := v_trx_header_id;
            l_trx_lines_tbl (1).trx_line_id                     := ra_customer_trx_lines_s.nextval;
            l_trx_lines_tbl (1).line_number                     := 1;
            l_trx_lines_tbl (1).description                     := 'Intercompany transfer from '||i.source_entity ||' to '||i.transfered_entity     ;
            l_trx_lines_tbl (1).memo_line_id                    := v_memo_line_id;
            l_trx_lines_tbl (1).quantity_invoiced               := 1;
            l_trx_lines_tbl (1).unit_selling_price              := i.APPLY_AMOUNT;
            l_trx_lines_tbl (1).line_type                       := 'LINE';
            

fnd_file.put_line(fnd_file.LOG, '--------------------APPS INITIALIZATION--------------------');
 select  decode(i.SOURCE_OU_ID , 82 , 50888 ,424 , 52413,425 ,52419) into l_responsiblity_id from dual;

FND_GLOBAL.Apps_Initialize(FND_GLOBAL.USER_ID,l_responsiblity_id,222);

mo_global.set_policy_context ('S',i.SOURCE_OU_ID );
MO_GLOBAL.INIT('AR');
fnd_file.put_line(fnd_file.LOG, '------------------------API  STARTS------------------------');

if l_status = 'DNS' then
   ar_invoice_api_pub.create_single_invoice ( -- std parameters
                                            p_api_version            => 1.0
                                           ,p_init_msg_list          => fnd_api.g_false
                                           ,p_commit                 => fnd_api.g_false
                                           -- api parameters
                                           ,p_batch_source_rec       => l_batch_source_rec
                                           ,p_trx_header_tbl         => l_trx_header_tbl
                                           ,p_trx_lines_tbl          => l_trx_lines_tbl
                                           ,p_trx_dist_tbl           => l_trx_dist_tbl
                                           ,p_trx_salescredits_tbl   => l_trx_salescredits_tbl
                                           -- Out parameters
                                           ,x_customer_trx_id        => l_customer_trx_id
                                           ,x_return_status          => o_return_status
                                           ,x_msg_count              => o_msg_count
                                           ,x_msg_data               => o_msg_data);



fnd_file.put_line(fnd_file.LOG, 'API RETURN STATUS          - '||o_return_status);                                           
fnd_file.put_line(fnd_file.LOG, '------------------------API  ENDS-------------------------- l_customer_trx_id '||l_customer_trx_id); 
commit;




    for i in (select  
TRX_HEADER_ID ,
TRX_LINE_ID,
TRX_SALESCREDIT_ID,
TRX_DIST_ID,
ERROR_MESSAGE,
INVALID_VALUE,
TRX_CONTINGENCY_ID from ar_trx_errors_gt)

loop
fnd_file.put_line(fnd_file.LOG,'  TRX_HEADER_ID '||i.TRX_HEADER_ID  );

fnd_file.put_line(fnd_file.LOG,'  TRX_LINE_ID '||i.TRX_LINE_ID  );

fnd_file.put_line(fnd_file.LOG,'  TRX_SALESCREDIT_ID '||i.TRX_SALESCREDIT_ID  );

 fnd_file.put_line(fnd_file.LOG,'  TRX_DIST_ID '||i.TRX_DIST_ID  );
fnd_file.put_line(fnd_file.LOG,'  ERROR_MESSAGE '||i.ERROR_MESSAGE  );
fnd_file.put_line(fnd_file.LOG,'  INVALID_VALUE '||i.INVALID_VALUE  );
fnd_file.put_line(fnd_file.LOG,'  TRX_CONTINGENCY_ID '||i.TRX_CONTINGENCY_ID  );



end loop; 

 IF o_return_status = fnd_api.g_ret_sts_error
        OR o_return_status = fnd_api.g_ret_sts_unexp_error THEN
 l_error_desc := o_msg_data  ;

        IF o_msg_count > 0 THEN
fnd_file.put_line(fnd_file.LOG, 'API MESSAGE COUNT   		- '||o_msg_count);
            FOR v_index IN 1 .. o_msg_count
            LOOP
            fnd_msg_pub.get (p_msg_index       => v_index
                            ,p_encoded         => 'F'
                            ,p_data            => o_msg_data
                            ,p_msg_index_out   => l_msg_index_out);
            o_msg_data := substr (o_msg_data, 1, 3950);



l_status:= 'DNE';
l_error_desc :=l_error_desc || o_msg_data;
 l_status_desc:= 'Debit Note Error';
--
--            update xx_ar_transactions_stg set 
--            STATUS_FLAG = 'DNE', ERROR_MSG = o_msg_data
--            where batch_number = i.batch_number
--            and Source_entity_number = i.source_entity_number
--            AND status_flag = 'V';

fnd_file.put_line(fnd_file.LOG, 'ERROR MESSAGE       	- '||o_msg_data);

            END LOOP;

        END IF;

      ELSE

        SELECT count (*) INTO l_cnt FROM ar_trx_errors_gt;  
        
        
       

        IF l_cnt = 0 THEN

          BEGIN
            SELECT trx_number
            INTO   l_trx_number
            FROM   ra_customer_trx_all
            WHERE  customer_trx_id = l_customer_trx_id;
          END;

for j in (
select SOURCE_ENTITY_NUMBER , transfered_ENTITY_NUMBER,TRANSFERED_CUSTOMER_CODE,TRANSFERED_CUST_ACC_ID from xx_ar_transactions_stg 
where batch_number =  i.batch_number
and  source_customer_code = i.source_customer_code
and SOURCE_ENTITY = i.SOURCE_ENTITY
and status_flag = 'V'
)
loop
          insert into  xx_ar_transactions_dtl
          (
          batch_number,
          SOURCE_ENTITY_NUMBER,
          SOURCE_ENTITY,
          TRANSFERED_ENTITY,
          DN_TRX_NUMBER,
          LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, CREATED_BY,CREATION_DATE ,
          transfered_ENTITY_NUMBER , TRANSFERED_CUSTOMER_CODE,TRANSFERED_CUST_ACC_ID , source_customer_code
          )
          values
          (
          i.batch_number,
          j.SOURCE_ENTITY_NUMBER,
          i.SOURCE_ENTITY,
          i.TRANSFERED_ENTITY,
          l_trx_number,
          sysdate,
          fnd_global.user_id,
          fnd_global.user_id,
          fnd_global.user_id
          ,sysdate
          ,j.transfered_ENTITY_NUMBER , j.TRANSFERED_CUSTOMER_CODE, j.TRANSFERED_CUST_ACC_ID , i.source_customer_code
          );
          
          end loop;
          

        ELSE 



l_status:= 'DNE';
l_error_desc :=l_error_desc || 'Transaction not Created, Please check Oracle ar_trx_errors_gt table trx_header_id '||v_trx_header_id;
 l_status_desc:= 'Debit Note Error';
--            update xx_ar_transactions_stg set 
--            STATUS_FLAG = 'DNE', ERROR_MSG = 'Transaction not Created, Please check Oracle ar_trx_errors_gt table trx_header_id '||v_trx_header_id
--            where batch_number = i.batch_number
--            and Source_entity_number = i.source_entity_number
--            AND status_flag = 'V';
fnd_file.put_line(fnd_file.LOG, 'Transaction not Created, Please check Oracle ar_trx_errors_gt table  trx_header_id'||v_trx_header_id);

--        BEGIN
--            
--            SELECT LISTAGG(error_message, ',') WITHIN GROUP (ORDER BY error_message) Error_message
--              INTO l_error_message 
--              FROM ar_trx_errors_gt
--             WHERE trx_header_id = v_trx_header_id
--            GROUP BY  trx_header_id;
--              
--        EXCEPTION WHEN OTHERS THEN 
--        
--        l_error_message := NULL;
--         
--        END ;
--        


        END IF;





    END IF; 

    end if;

            update xx_ar_transactions_stg set 
            STATUS_FLAG = l_status,
             DN_STATUS =l_status_desc,
            ERROR_MSG = l_error_desc
            where batch_number = i.batch_number
            and source_customer_code = i.source_customer_code
            AND status_flag = 'V';    

end loop;
EXCEPTION WHEN OTHERS THEN 
fnd_file.put_line(fnd_file.LOG, ' EXCEPTION occurs debit note '||SQLERRM);

end DN_creation_API;


procedure CN_creation_API(p_batch_number in varchar2)
as

cursor c1 is


    
 SELECT
   sum( apply_amount) apply_amount,
    batch_number,
    ic_type,
    source_entity,
--    source_customer_code,
--    source_entity_number,
    transfered_entity,
    TRANSFERED_OU_ID, 
    TRANSFERED_CUST_ACC_ID,
    TRANSFERED_CUSTOMER_CODE,
    CURRENCY
    FROM
    xx_ar_transactions_stg
WHERE
    batch_number = p_batch_number
    AND status_flag ='DNAS'
    group by      batch_number,
    ic_type,
    source_entity,
--    source_customer_code,
--    source_entity_number,
    transfered_entity,
    TRANSFERED_OU_ID, 
    TRANSFERED_CUST_ACC_ID,TRANSFERED_CUSTOMER_CODE,CURRENCY;



--    TRANSFERED_ENTITY_ID;

 v_memo_line_id          NUMBER          DEFAULT NULL;
v_trx_header_id         NUMBER          DEFAULT NULL;
l_customer_trx_id       NUMBER;
o_return_status         VARCHAR2(1);
o_msg_count             NUMBER;
o_msg_data              VARCHAR2(2000);
 l_cnt                   NUMBER          DEFAULT 0;
l_msg_index_out         NUMBER;
l_trx_number            VARCHAR2(100);
l_status varchar2(20);
l_error_desc  varchar2(2000);
v_batch_source_id       ra_batch_sources_all.batch_source_id%TYPE DEFAULT NULL;
l_batch_source_rec      ar_invoice_api_pub.batch_source_rec_type;
l_memo_line_name  varchar2(200);
 l_cust_trx_type_id    ra_customer_trx_all.CUST_TRX_TYPE_ID%TYPE DEFAULT NULL;
l_trx_header_tbl        ar_invoice_api_pub.trx_header_tbl_type;
l_trx_lines_tbl         ar_invoice_api_pub.trx_line_tbl_type;
l_trx_dist_tbl          ar_invoice_api_pub.trx_dist_tbl_type;
l_trx_salescredits_tbl  ar_invoice_api_pub.trx_salescredits_tbl_type;
 l_status_desc varchar2(200);
 l_closing_status  varchar2(200);
 l_responsiblity_id number;
l_date date;
l_exchange_rate   number;
l_ct_ref varchar2(200);
begin

for i in c1 
loop
l_status_desc:= 'Success';
l_status := 'CNS';
l_error_desc  := null;
l_closing_status := null;
l_date := null;
l_responsiblity_id := null;
l_exchange_rate := null;
l_ct_ref :=  null;
   BEGIN

        SELECT ra_customer_trx_s.nextval 
          INTO v_trx_header_id 
          FROM dual;

        EXCEPTION WHEN OTHERS THEN

        l_status := 'CNE';
        l_error_desc :=l_error_desc || ' error while getting the ra_customer_trx_s sequence ' ;
         l_status_desc:= 'Credit Note Error';
        END ;

begin
select BATCH_SOURCE_ID into v_batch_source_id
from ra_batch_sources_all
where upper(name) like upper('IC-RT')
and org_id =i.TRANSFERED_OU_ID ;
exception when others then
l_status:= 'CNE';
l_error_desc :=l_error_desc || ' error while getting the batch source ' ;
         l_status_desc:= 'Credit Note Error';

end;



--v_batch_source_id :=13003;


begin

select MEANING into l_memo_line_name 
from fnd_lookup_values 
where lookup_type = 'XX_AR_TRANS_MEMO_LINE' 
and DESCRIPTION = i.source_entity
and tag = i.TRANSFERED_ENTITY;


SELECT    t.memo_line_id into v_memo_line_id
  FROM ar_memo_lines_all_tl t
      ,ar_memo_lines_all_b b
 WHERE  b.memo_line_id = t.memo_line_id
   AND  t.name =   l_memo_line_name
   and b.org_id  =i.TRANSFERED_OU_ID ;

exception when others then

l_status:= 'CNE';
l_error_desc :=l_error_desc || ' error while getting the  MEMO LINE NAME ' ;
         l_status_desc:= 'Credit Note Error';

end;





begin


select rctta.CUST_TRX_TYPE_ID into l_cust_trx_type_id  from  ra_cust_trx_types_all rctta,
gl_code_combinations_kfv gcc
where rctta.GL_ID_REV = gcc.code_combination_id
and  gcc.segment2 
in (

select gcc.segment2 from  ra_cust_trx_types_all rctta,
gl_code_combinations_kfv gcc
where rctta.GL_ID_REV = gcc.code_combination_id
and org_id  = i.TRANSFERED_OU_ID
)and rownum = 1   and rctta.org_id= i.TRANSFERED_OU_ID   and  rctta.NAME  like  '%-CM';


--l_cust_trx_type_id:= 1050;
dbms_output.put_line('l_cust_trx_type_id   '||l_cust_trx_type_id);

end ;





SELECT
    closing_status into l_closing_status 
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id =i.TRANSFERED_OU_ID )
AND SYSDATE BETWEEN start_date AND end_date;




if l_closing_status = 'O'  then

l_date := sysdate;
else
begin
SELECT
   max(END_DATE)  into l_date
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id = i.TRANSFERED_OU_ID)
and closing_status = 'O';

exception when others then
l_date := sysdate;
end;

end if; 




if i.CURRENCY = 'INR' then
fnd_file.put_line(fnd_file.LOG,  '  while getting the INR  CURRENCY '||i.CURRENCY); 
 l_trx_header_tbl (1).exchange_rate_type := null;  --Corporate
   l_trx_header_tbl (1).trx_currency           := null;
null;
else


fnd_file.put_line(fnd_file.LOG,  '  while getting the  CURRENCY '||i.CURRENCY); 

 l_trx_header_tbl (1).exchange_rate_type := 'Corporate';  --Corporate
   l_trx_header_tbl (1).trx_currency       := i.CURRENCY;
 begin
 
  fnd_file.put_line(fnd_file.LOG,  '    in getting l_date '||l_date); 

-- 
--   select CONVERSION_RATE into l_exchange_rate  from 
--gl_daily_rates
--where 
--FROM_CURRENCY= i.currency
--and TO_CURRENCY = 'INR'
-- and to_date(CONVERSION_DATE) = TO_DATE(l_date);
-- fnd_file.put_line(fnd_file.LOG,  '    in getting l_exchange_rate '||l_exchange_rate); 
--
-- l_trx_header_tbl (1).exchange_rate := l_exchange_rate;
 
 
 
 exception when others then
 
 
fnd_file.put_line(fnd_file.LOG,  '  Error in getting CONVERSION_RATE '||sqlerrm); 
  l_status_desc:= 'Credit Note Error';
l_status:= 'CNE';
l_error_desc :=l_error_desc || ' error while getting the  CONVERSION_RATE ' ;

 
 end;
 
 

end if;



begin

select  source_entity_number into l_ct_ref
FROM
    xx_ar_transactions_stg
WHERE
    batch_number = p_batch_number
    AND SOURCE_CUST_ACC_ID =  i.TRANSFERED_CUST_ACC_ID
    and rownum =1 ;

exception when others then

l_ct_ref :=  null;

end;





            l_trx_header_tbl (1).trx_header_id                  := v_trx_header_id;
            l_trx_header_tbl (1).trx_number                     := NULL;
            l_trx_header_tbl (1).bill_to_customer_id            := i.TRANSFERED_CUST_ACC_ID;
            l_trx_header_tbl (1).cust_trx_type_id               := l_cust_trx_type_id;
            l_trx_header_tbl (1).trx_date               := l_date;
            l_trx_header_tbl (1).gl_date               := l_date;
            l_trx_header_tbl (1).comments                      := 'Intercompany transfer from '||i.source_entity ||' to '||i.transfered_entity    ;
             
             l_trx_header_tbl (1).interface_header_attribute1     :='RT-'||i.source_entity ||'-'||i.transfered_entity    ;
             l_trx_header_tbl (1).reference_number :=  'RT-'||i.source_entity ||'-'||i.transfered_entity    ;
            l_batch_source_rec.batch_source_id                  := v_batch_source_id;
              l_trx_header_tbl (1).interface_header_attribute15   := 'TN - CHENNAI:Bill To';
            l_trx_lines_tbl (1).trx_header_id                   := v_trx_header_id;
            l_trx_lines_tbl (1).trx_line_id                     := ra_customer_trx_lines_s.nextval;
            l_trx_lines_tbl (1).line_number                     := 1;
            l_trx_lines_tbl (1).description                    := 'Intercompany transfer from '||i.source_entity ||' to '||i.transfered_entity    ;
            l_trx_lines_tbl (1).memo_line_id                    := v_memo_line_id;
            l_trx_lines_tbl (1).quantity_invoiced               := 1;
            l_trx_lines_tbl (1).unit_selling_price              := -1 *  (i.APPLY_AMOUNT);
            l_trx_lines_tbl (1).line_type                       := 'LINE';

fnd_file.put_line(fnd_file.LOG, '--------------------APPS INITIALIZATION--------------------');
mo_global.set_policy_context ('S',i.TRANSFERED_OU_ID );
 select  decode(i.TRANSFERED_OU_ID , 82 , 50888 ,424 , 52413,425 ,52419) into l_responsiblity_id from dual;

FND_GLOBAL.Apps_Initialize(FND_GLOBAL.USER_ID,l_responsiblity_id,222);
MO_GLOBAL.INIT('AR');
fnd_file.put_line(fnd_file.LOG, '------------------------API  STARTS------------------------ v_trx_header_id '||v_trx_header_id);
fnd_file.put_line(fnd_file.LOG, '------------------------API  STARTS------------------------ i.APPLY_AMOUNT '||i.APPLY_AMOUNT);


if l_status = 'CNS' then
   ar_invoice_api_pub.create_single_invoice ( -- std parameters
                                            p_api_version            => 1.0
                                           ,p_init_msg_list          => fnd_api.g_false
                                           ,p_commit                 => fnd_api.g_false
                                           -- api parameters
                                           ,p_batch_source_rec       => l_batch_source_rec
                                           ,p_trx_header_tbl         => l_trx_header_tbl
                                           ,p_trx_lines_tbl          => l_trx_lines_tbl
                                           ,p_trx_dist_tbl           => l_trx_dist_tbl
                                           ,p_trx_salescredits_tbl   => l_trx_salescredits_tbl
                                           -- Out parameters
                                           ,x_customer_trx_id        => l_customer_trx_id
                                           ,x_return_status          => o_return_status
                                           ,x_msg_count              => o_msg_count
                                           ,x_msg_data               => o_msg_data);

fnd_file.put_line(fnd_file.LOG, 'API RETURN STATUS          - '||o_return_status);                                           
fnd_file.put_line(fnd_file.LOG, '------------------------API  ENDS--------------------------'); 

fnd_file.put_line(fnd_file.LOG, 'API RETURN STATUS o_msg_count         - '||o_msg_count);                                           

fnd_file.put_line(fnd_file.LOG, 'org_id          - '||fnd_global.org_id);                                           

fnd_file.put_line(fnd_file.LOG, 'API RETURN STATUS          - '||o_return_status);                                           


commit;
 IF o_return_status = fnd_api.g_ret_sts_error
        OR o_return_status = fnd_api.g_ret_sts_unexp_error THEN
 l_error_desc := o_msg_data  ;

        IF o_msg_count > 0 THEN
fnd_file.put_line(fnd_file.LOG, 'API MESSAGE COUNT   		- '||o_msg_count);
            FOR v_index IN 1 .. o_msg_count
            LOOP
            fnd_msg_pub.get (p_msg_index       => v_index
                            ,p_encoded         => 'F'
                            ,p_data            => o_msg_data
                            ,p_msg_index_out   => l_msg_index_out);
            o_msg_data := substr (o_msg_data, 1, 3950);


            END LOOP;

        END IF;
        
        
        
l_status:= 'CNE';
l_error_desc :=l_error_desc || o_msg_data ;
         l_status_desc:= 'Credit Note Error';

fnd_file.put_line(fnd_file.LOG, 'ERROR MESSAGE       	- '||o_msg_data);




      ELSE

        SELECT count (*) INTO l_cnt FROM ar_trx_errors_gt;   

        IF l_cnt = 0 THEN

          BEGIN
            SELECT trx_number
            INTO   l_trx_number
            FROM   ra_customer_trx_all
            WHERE  customer_trx_id = l_customer_trx_id;
            fnd_file.put_line(fnd_file.LOG, 'trx_number  '||l_trx_number);

          END;


          update xx_ar_transactions_dtl set CN_TRX_NUMBER = l_trx_number ,
          LAST_UPDATE_DATE = sysdate,
          LAST_UPDATED_BY = fnd_global.user_id,
          LAST_UPDATE_LOGIN = fnd_global.user_id,
          CREATED_BY = fnd_global.user_id,
          CREATION_DATE  = sysdate
          where 
          batch_number = i.batch_number
--          and SOURCE_ENTITY_NUMBER = i.SOURCE_ENTITY_NUMBER
--          and SOURCE_ENTITY= i.SOURCE_ENTITY
          and TRANSFERED_CUST_ACC_ID = i.TRANSFERED_CUST_ACC_ID
          and TRANSFERED_CUSTOMER_CODE  = i.TRANSFERED_CUSTOMER_CODE
          ;





        ELSE 
        l_status:= 'CNE';
        l_error_desc := l_error_desc ||  'Transaction not Created, Please check Oracle ar_trx_errors_gt table trx_header_id '||v_trx_header_id;
fnd_file.put_line(fnd_file.LOG, 'Transaction not Created, Please check Oracle ar_trx_errors_gt table  trx_header_id'||v_trx_header_id);


     l_status_desc:= 'Credit Note Error';
--        BEGIN
--            
--            SELECT LISTAGG(error_message, ',') WITHIN GROUP (ORDER BY error_message) Error_message
--              INTO l_error_message 
--              FROM ar_trx_errors_gt
--             WHERE trx_header_id = v_trx_header_id
--            GROUP BY  trx_header_id;
--              
--        EXCEPTION WHEN OTHERS THEN 
--        
--        l_error_message := NULL;
--         
--        END ;
--        


        END IF;





    END IF; 
       END IF; 


            update xx_ar_transactions_stg set 
            STATUS_FLAG = l_status,
            ERROR_MSG = l_error_desc
            ,   CN_STATUS =l_status_desc
            where batch_number = i.batch_number
            and TRANSFERED_CUSTOMER_CODE = i.TRANSFERED_CUSTOMER_CODE
            AND status_flag = 'DNAS';    


    for i in (select  
TRX_HEADER_ID ,
TRX_LINE_ID,
TRX_SALESCREDIT_ID,
TRX_DIST_ID,
ERROR_MESSAGE,
INVALID_VALUE,
TRX_CONTINGENCY_ID from ar_trx_errors_gt)

loop
fnd_file.put_line(fnd_file.LOG,'  TRX_HEADER_ID '||i.TRX_HEADER_ID  );

fnd_file.put_line(fnd_file.LOG,'  TRX_LINE_ID '||i.TRX_LINE_ID  );

fnd_file.put_line(fnd_file.LOG,'  TRX_SALESCREDIT_ID '||i.TRX_SALESCREDIT_ID  );

 fnd_file.put_line(fnd_file.LOG,'  TRX_DIST_ID '||i.TRX_DIST_ID  );
fnd_file.put_line(fnd_file.LOG,'  ERROR_MESSAGE '||i.ERROR_MESSAGE  );
fnd_file.put_line(fnd_file.LOG,'  INVALID_VALUE '||i.INVALID_VALUE  );
fnd_file.put_line(fnd_file.LOG,'  TRX_CONTINGENCY_ID '||i.TRX_CONTINGENCY_ID  );



end loop;



end loop;
EXCEPTION WHEN OTHERS THEN 
fnd_file.put_line(fnd_file.LOG, ' EXCEPTION occurs '||SQLERRM);



end CN_creation_API;

procedure Receipt_application_API(p_batch_number in varchar2)
as
cursor c1 
is
select BATCH_NUMBER, SOURCE_ENTITY_NUMBER, SOURCE_ENTITY, TRANSFERED_ENTITY, DN_TRX_NUMBER, CN_TRX_NUMBER, SOURCE_OU_ID, sum(APPLY_AMOUNT) APPLY_AMOUNT 
   from
   (
    select distinct  dtl.BATCH_NUMBER,  dtl.SOURCE_ENTITY_NUMBER,  dtl.SOURCE_ENTITY, 
 dtl.TRANSFERED_ENTITY,  dtl.DN_TRX_NUMBER,  dtl.CN_TRX_NUMBER ,stg.SOURCE_OU_ID ,  (APPLY_AMOUNT) APPLY_AMOUNT
from xx_ar_transactions_dtl dtl , xx_ar_transactions_stg stg 
where dtl.BATCH_NUMBER = stg.BATCH_NUMBER
and dtl.SOURCE_ENTITY_NUMBER  = stg.SOURCE_ENTITY_NUMBER
and dtl.SOURCE_ENTITY  = stg.SOURCE_ENTITY
and dtl.TRANSFERED_ENTITY = stg.TRANSFERED_ENTITY
and stg.status_flag = 'DNS'
and stg.batch_number = p_batch_number
and stg.transfered_ENTITY_NUMBER  =dtl.transfered_ENTITY_NUMBER )
group by 
BATCH_NUMBER, SOURCE_ENTITY_NUMBER, SOURCE_ENTITY, TRANSFERED_ENTITY, DN_TRX_NUMBER, CN_TRX_NUMBER, SOURCE_OU_ID 
 ;

--
--select distinct  dtl.BATCH_NUMBER,  dtl.SOURCE_ENTITY_NUMBER,  dtl.SOURCE_ENTITY, 
-- dtl.TRANSFERED_ENTITY,  dtl.DN_TRX_NUMBER,  dtl.CN_TRX_NUMBER ,stg.SOURCE_OU_ID ,  sum(APPLY_AMOUNT) APPLY_AMOUNT
--from xx_ar_transactions_dtl dtl , xx_ar_transactions_stg stg 
--where dtl.BATCH_NUMBER = stg.BATCH_NUMBER
--and dtl.SOURCE_ENTITY_NUMBER  = stg.SOURCE_ENTITY_NUMBER
--and dtl.SOURCE_ENTITY  = stg.SOURCE_ENTITY
--and dtl.TRANSFERED_ENTITY = stg.TRANSFERED_ENTITY
--and stg.status_flag = 'DNS'
--and stg.batch_number = p_batch_number
--and stg.transfered_ENTITY_NUMBER  =dtl.transfered_ENTITY_NUMBER
--group by  dtl.BATCH_NUMBER,  dtl.SOURCE_ENTITY_NUMBER,  dtl.SOURCE_ENTITY, 
-- dtl.TRANSFERED_ENTITY,  dtl.DN_TRX_NUMBER,  dtl.CN_TRX_NUMBER ,stg.SOURCE_OU_ID 
-- ;

l_status varchar2(20);
l_error_desc  varchar2(2000);
     l_status_desc  varchar2(200);
l_cust_trx_id  number;
p_cash_receipt_id  number;
v_msg_count  number;
p_amount_applied  number;
v_request_id  number;
l_closing_status varchar2(200);
l_responsiblity_id number;
l_inv_apply_amt   number;

l_date  date;
begin


for i in c1
loop
l_status:= 'DNAS';
l_error_desc:= null;
l_date:=  null;
l_closing_status:= null;
l_responsiblity_id :=  null;
     l_status_desc:= 'Debit Note Application Success';
     l_inv_apply_amt:=  null;
	BEGIN -- Verification of cash receipt id for given receipt*/

    	  	          	   Dbms_output.put_line('Verification of cash receipt id for given receipt ');

              SELECT cash_receipt_id
              INTO p_cash_receipt_id
              FROM ar_cash_receipts_all cash_rect
              WHERE cash_rect.receipt_number LIKE i.SOURCE_ENTITY_NUMBER
			  and org_id = i.SOURCE_OU_ID;
     EXCEPTION
            WHEN others THEN
             p_cash_receipt_id := null;
			 l_status 	:= 'DNAE';
             l_status_desc:= 'Debit Note Application Error';
			l_error_desc  	:= l_error_desc || ', Invalid RECEIPT NUMBER ';
fnd_file.put_line(fnd_file.LOG,', Invalid RECEIPT NUMBER ');

    END;

begin



   SELECT    rcta.CUSTOMER_TRX_ID, nvl(AMOUNT_DUE_REMAINING,0)   into l_cust_trx_id,l_inv_apply_amt
                 FROM ra_customer_trx_all rcta ,
                 ar_payment_schedules_all ps
                WHERE 
                ps.CUSTOMER_TRX_ID = rcta.CUSTOMER_TRX_ID
                and rcta.trx_number = i.DN_TRX_NUMBER
                and rcta.org_id = i.SOURCE_OU_ID;


--select customer_Trx_id into l_cust_trx_id from ra_customer_trx_all where trx_number = i.DN_TRX_NUMBER
--and org_id =  i.SOURCE_OU_ID ;

Exception when others then
    l_cust_trx_id := null;
			 l_status 	:= 'DNAE';
			l_error_desc  	:= l_error_desc || ', Invalid Invoice NUMBER ';
                  	  	         fnd_file.put_line(fnd_file.LOG,'Invalid Invoice NUMBER ');
l_status_desc:= 'Debit Note Application Error';
end;





SELECT
    closing_status into l_closing_status 
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id =i.SOURCE_OU_ID )
AND SYSDATE BETWEEN start_date AND end_date;




if l_closing_status = 'O'  then

l_date := sysdate;
else
begin
SELECT
   max(END_DATE)  into l_date
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id = i.SOURCE_OU_ID)
and closing_status = 'O';

exception when others then
l_date := sysdate;
end;

end if; 




    mo_global.set_policy_context ('S',i.SOURCE_OU_ID );

   select  decode(i.SOURCE_OU_ID , 82 , 50888 ,424 , 52413,425 ,52419) into l_responsiblity_id from dual;

FND_GLOBAL.Apps_Initialize(FND_GLOBAL.USER_ID,l_responsiblity_id,222);


 fnd_file.put_line(fnd_file.LOG,'org_id  '|| fnd_global.org_id);
IF p_cash_receipt_id IS NOT NULL and l_status 	= 'DNAS' and l_cust_trx_id is not null THEN
              /*EXECUTE APPLICATION PROCEDURE*/
--              l_err_status  := NULL;
              v_msg_count := 0;

              fnd_msg_pub.initialize;
              p_amount_applied := i.APPLY_AMOUNT;
--    p_amount_applied := l_inv_apply_amt;
              
              ar_receipt_api_pub.apply (p_api_version => 1.0, 
              p_init_msg_list => fnd_api.g_true,
              x_return_status => l_status,
              x_msg_count => v_msg_count,
              x_msg_data => l_error_desc,
              p_cash_receipt_id => p_cash_receipt_id,
              p_trx_number => i.DN_TRX_NUMBER, 
             p_customer_Trx_id => l_cust_trx_id,
			  p_amount_applied=> p_amount_applied,
             p_org_id =>         i.SOURCE_OU_ID   ,
             p_apply_date                       => l_date,
              p_apply_gl_date                    =>l_date
             );


	IF l_status              <> 'S' THEN
                IF (fnd_msg_pub.count_msg > 0) THEN
                  FOR ln_i               IN 1 .. fnd_msg_pub.count_msg
                  LOOP
                    fnd_msg_pub.get
                    (
                      p_msg_index => ln_i, p_data => l_error_desc, p_encoded => 'F', p_msg_index_out => v_msg_count
                    )
                    ;
          fnd_file.put_line(fnd_file.LOG, RPAD (i.SOURCE_ENTITY_NUMBER, 30, ' ' ) || '  ' 
                    || RPAD (i.DN_TRX_NUMBER, 30, ' ' ) || '  ' || RPAD (l_status, 10, ' ') || '  ' || 
                    RPAD (p_amount_applied, 15, ' ' ) || '  ' || l_error_desc );
                   fnd_file.put_line(fnd_file.LOG, '         ');

                  END LOOP;

     END IF;
          else

          l_status:= 'DNAS';
 end if;

 end if;    

      update xx_ar_transactions_stg set 
            STATUS_FLAG = l_status,
            ERROR_MSG = l_error_desc

--           status_desc =  l_status_desc 
            where batch_number = i.batch_number
            and Source_entity_number = i.source_entity_number
            AND status_flag = 'DNS';  


end loop;
 COMMIT;

--  v_request_id := fnd_request.submit_request ('AR', 'ARAPPLNEXP', NULL, SYSDATE, NULL, v_request_id );
--  dbms_output.put_line('v_request_id '||v_request_id);
--  fnd_file.put_line(fnd_file.LOG, 'v_request_id '||v_request_id);


end Receipt_application_API;


procedure CM_application_API(p_batch_number in varchar2)
as
cursor c1 
is
--
--select  dtl.BATCH_NUMBER,    dtl.SOURCE_ENTITY, 
-- dtl.TRANSFERED_ENTITY,    dtl.CN_TRX_NUMBER ,stg.SOURCE_OU_ID ,
-- stg.transfered_ou_id, sum(stg.APPLY_AMOUNT) APPLY_AMOUNT, stg.TRANSFERED_ENTITY_NUMBER
--from xx_ar_transactions_dtl dtl , xx_ar_transactions_stg stg 
--where dtl.BATCH_NUMBER = stg.BATCH_NUMBER
--and dtl.SOURCE_ENTITY_NUMBER  = stg.SOURCE_ENTITY_NUMBER
--and dtl.SOURCE_ENTITY  = stg.SOURCE_ENTITY
--and dtl.TRANSFERED_ENTITY = stg.TRANSFERED_ENTITY
--and stg.batch_number = p_batch_number
--and dtl.TRANSFERED_ENTITY_NUMBER  = stg.TRANSFERED_ENTITY_NUMBER
--AND status_flag = 'CNS'
--group  by 
--dtl.BATCH_NUMBER,    dtl.SOURCE_ENTITY, 
-- dtl.TRANSFERED_ENTITY,    dtl.CN_TRX_NUMBER ,stg.SOURCE_OU_ID ,
-- stg.transfered_ou_id,   stg.TRANSFERED_ENTITY_NUMBER
--;





select
BATCH_NUMBER, SOURCE_ENTITY, TRANSFERED_ENTITY, CN_TRX_NUMBER, SOURCE_OU_ID, TRANSFERED_OU_ID, sum( APPLY_AMOUNT) APPLY_AMOUNT, TRANSFERED_ENTITY_NUMBER
from
(

select distinct dtl.BATCH_NUMBER,    dtl.SOURCE_ENTITY, 
 dtl.TRANSFERED_ENTITY,    dtl.CN_TRX_NUMBER ,stg.SOURCE_OU_ID ,
 stg.transfered_ou_id, (stg.APPLY_AMOUNT) APPLY_AMOUNT, stg.TRANSFERED_ENTITY_NUMBER
from xx_ar_transactions_dtl dtl , xx_ar_transactions_stg stg 
where dtl.BATCH_NUMBER = stg.BATCH_NUMBER
and dtl.SOURCE_ENTITY_NUMBER  = stg.SOURCE_ENTITY_NUMBER
and dtl.SOURCE_ENTITY  = stg.SOURCE_ENTITY
and dtl.TRANSFERED_ENTITY = stg.TRANSFERED_ENTITY
and stg.batch_number =  p_batch_number
AND status_flag = 'CNS'
and dtl.TRANSFERED_ENTITY_NUMBER  = stg.TRANSFERED_ENTITY_NUMBER)
group  by  BATCH_NUMBER, SOURCE_ENTITY, TRANSFERED_ENTITY, CN_TRX_NUMBER, SOURCE_OU_ID, TRANSFERED_OU_ID, TRANSFERED_ENTITY_NUMBER
;


   --local in Variables
   v_cm_payment_schedule         NUMBER;
-- := 101502; -- Payment Schedule ID of Credit Memo from APPS.AR_PAYMENT_SCHEDULES_ALL
   v_inv_payment_schedule        NUMBER;
-- := 101500; -- Payment Schedule ID of Invoice from APPS.AR_PAYMENT_SCHEDULES_ALL
   v_amount_applied              NUMBER;
   -- := 1000; -- Amount of credit memo to apply to invoice
   v_apply_date                  DATE            := null;
   v_gl_date                     DATE            :=null;
   v_ussgl_transaction_code      VARCHAR2 (1024);
   -- null, but check AR_RECEIVABLE_APPLICATIONS_ALL
   v_null_flex                   VARCHAR2 (1024);
   -- null, unless you have flexfield segments to define
   v_customer_trx_line_id        NUMBER;
   -- null, but check AR_RECEIVABLE_APPLICATIONS_ALL
   v_comments                    VARCHAR2 (240)  := 'Applied automatically';
   v_module_name                 VARCHAR2 (128)  := 'AR';
   -- If null,   validation won 't occur
   v_module_version              VARCHAR2 (128)  := ' 1 ';
                                          -- If null, validation won' t occur
   --Out parameters
   v_out_rec_application_id      NUMBER;
   v_acctd_amount_applied_from   NUMBER;
   v_acctd_amount_applied_to     NUMBER;
   v_cm_due_amount               NUMBER;
   v_inv_due_amount              NUMBER;
   v_inv_count                   NUMBER;
   v_cm_count                    NUMBER;
   l_org                         NUMBER          := 2;
   x_err_flag                    VARCHAR (1);
   x_err_msg                      VARCHAR2 (2000) ;
   l_inv_apply_amt   number;

   l_status varchar2(20);
l_error_desc  varchar2(2000);
l_status_desc varchar2(200);
l_date  date;
l_closing_status  varchar2(200);

begin

fnd_file.put_line(fnd_file.LOG,',  Credit Note Application starts');



for i in c1
loop
    mo_global.set_policy_context ('S',i.transfered_ou_id );
 l_status := 'CNAS';
 l_error_desc := null;
     l_status_desc:= 'Success';
    l_closing_status:=  null;
 l_date := null;
l_inv_apply_amt :=  null;

     BEGIN
--         SELECT   aps.payment_schedule_id
--           INTO  v_cm_payment_schedule
--           FROM ar_payment_schedules_all aps
--          WHERE 1 = 1
--            AND aps.trx_number = i.CN_TRX_NUMBER
--             and aps.org_id  = i.transfered_ou_id;--i.invoice_number;
--             
--             
             

   SELECT   ps.payment_schedule_id, nvl(AMOUNT_DUE_REMAINING,0) * (-1)   into v_cm_payment_schedule,l_inv_apply_amt
                 FROM ra_customer_trx_all rcta ,
                 ar_payment_schedules_all ps
                WHERE 
                ps.CUSTOMER_TRX_ID = rcta.CUSTOMER_TRX_ID
                and rcta.trx_number = i.CN_TRX_NUMBER
                and rcta.org_id = i.transfered_ou_id;
             
             FND_FILE.PUT_LINE(FND_FILE.LOG, ' l_inv_apply_amt '||l_inv_apply_amt);                                 

      EXCEPTION
         WHEN others
         THEN
            l_status := 'CNAE';
FND_FILE.PUT_LINE(FND_FILE.LOG, ' Error in CM No. is not DB');                                 
 l_error_desc:= l_error_desc ||  '  Error in CM No. is not DB ' ;
l_status_desc:= 'Credit Note Application Error';
dbms_output.put_line( ' Credit Note Application Error    ');   

      END;




         BEGIN
         SELECT   aps.payment_schedule_id
           INTO  v_inv_payment_schedule
           FROM ar_payment_schedules_all aps
          WHERE 1 = 1
            AND aps.trx_number = i.TRANSFERED_ENTITY_NUMBER
            and aps.org_id  = i.transfered_ou_id; --i.REFERENCE; 
            
            
            

            
      EXCEPTION
         WHEN  others then
   l_status := 'CNAE';
FND_FILE.PUT_LINE(FND_FILE.LOG, ' Error in invoice No.  ');                                 
 l_error_desc:= l_error_desc ||  '  Error in invoice No. ' ;
 l_status_desc:= 'Credit Note Application Error';
dbms_output.put_line( ' Error in invoice No.    ');   
      END;





SELECT
    closing_status into l_closing_status 
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id =i.transfered_ou_id )
AND SYSDATE BETWEEN start_date AND end_date;




if l_closing_status = 'O'  then

l_date := sysdate;
else
begin
SELECT
   max(END_DATE)  into l_date
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id = i.transfered_ou_id)
and closing_status = 'O';

exception when others then
l_date := sysdate;
end;

end if; 





if l_status  = 'CNAS'
then

dbms_output.put_line( 'CNAS   ');       
fnd_file.put_line(fnd_file.LOG,',  CNAS');
fnd_file.put_line(fnd_file.LOG,',  i.TRANSFERED_ENTITY_NUMBER '||i.TRANSFERED_ENTITY_NUMBER);
fnd_file.put_line(fnd_file.LOG,',  i.i.CN_TRX_NUMBER '|| i.CN_TRX_NUMBER);



fnd_file.put_line(fnd_file.LOG,', l_inv_apply_amt ' ||l_inv_apply_amt);
fnd_file.put_line(fnd_file.LOG,', i.APPLY_AMOUNT ' ||i.APPLY_AMOUNT);




begin
arp_process_application.cm_application
                 (p_cm_ps_id                       => v_cm_payment_schedule
                 ,p_invoice_ps_id                  => v_inv_payment_schedule
                 ,p_amount_applied                 => I.APPLY_AMOUNT    --i.APPLY_AMOUNT
                 ,p_apply_date                     => l_date
                 ,p_gl_date                        => l_date
                 ,p_ussgl_transaction_code         => NULL
                 ,p_attribute_category             => NULL
                 ,p_attribute1                     => NULL
                 ,p_attribute2                     => NULL
                 ,p_attribute3                     => NULL
                 ,p_attribute4                     => NULL
                 ,p_attribute5                     => NULL
                 ,p_attribute6                     => NULL
                 ,p_attribute7                     => NULL
                 ,p_attribute8                     => NULL
                 ,p_attribute9                     => NULL
                 ,p_attribute10                    => NULL
                 ,p_attribute11                    => NULL
                 ,p_attribute12                    => NULL
                 ,p_attribute13                    => NULL
                 ,p_attribute14                    => NULL
                 ,p_attribute15                    => NULL
                 ,p_global_attribute_category      => NULL
                 ,p_global_attribute1              => NULL
                 ,p_global_attribute2              => NULL
                 ,p_global_attribute3              => NULL
                ,p_global_attribute4               => NULL
                 ,p_global_attribute5              => NULL
                 ,p_global_attribute6              => NULL
                 ,p_global_attribute7              => NULL
                 ,p_global_attribute8              => NULL
                 ,p_global_attribute9              => NULL
                 ,p_global_attribute10             => NULL
                 ,p_global_attribute11             => NULL
                 ,p_global_attribute12             => NULL
                 ,p_global_attribute13             => NULL
                 ,p_global_attribute14             => NULL
                 ,p_global_attribute15             => NULL
                 ,p_global_attribute16             => NULL
                 ,p_global_attribute17             => NULL
                 ,p_global_attribute18             => NULL
                 ,p_global_attribute19             => NULL
                 ,p_global_attribute20             => NULL
                 ,p_customer_trx_line_id           => v_customer_trx_line_id
                 ,p_comments                       => v_comments
                 ,p_module_name                    => v_module_name
                 ,p_module_version                 => v_module_version
                 ,p_out_rec_application_id         => v_out_rec_application_id
                 ,p_acctd_amount_applied_from      => v_acctd_amount_applied_from
                 ,p_acctd_amount_applied_to        => v_acctd_amount_applied_to
                 );
             FND_FILE.PUT_LINE(FND_FILE.LOG, ' v_acctd_amount_applied_from '||v_acctd_amount_applied_from);                                 


             FND_FILE.PUT_LINE(FND_FILE.LOG, ' v_acctd_amount_applied_to '||v_acctd_amount_applied_to);                                 

                  FND_FILE.PUT_LINE(FND_FILE.LOG, 'v_out_rec_application_id '||v_out_rec_application_id );                                 
dbms_output.put_line( 'v_out_rec_application_id   '||v_out_rec_application_id);                                 

         DBMS_OUTPUT.put_line ('v_out_rec_application_id.');
         IF v_out_rec_application_id IS NOT NULL
         THEN
             DBMS_OUTPUT.put_line ('Committing.');


            COMMIT;
         ELSE
            DBMS_OUTPUT.put_line ('Rolling back.');
            l_status := 'CNAE';
            l_status_desc:= 'Credit Note Application Error';

            ROLLBACK;
         END IF;
commit;
exception when others
then
 l_status := 'CNAE';
FND_FILE.PUT_LINE(FND_FILE.LOG, ' Error in calling API   '||sqlerrm);     

dbms_output.put_line( ' Error in calling API   '||sqlerrm);                                 

 l_error_desc:= l_error_desc ||  '  Error in calling API . '||sqlerrm ;
 l_status_desc:= 'Credit Note Application Error';

end ;
end if;



      update xx_ar_transactions_stg set 
            STATUS_FLAG = l_status, ERROR_MSG = l_error_desc
           ,CN_STATUS =  l_status_desc 
            where batch_number = i.batch_number
--            and Source_entity_number = i.source_entity_number
            and TRANSFERED_ENTITY_NUMBER = i.TRANSFERED_ENTITY_NUMBER
            and transfered_ou_id= i.transfered_ou_id
            AND status_flag = 'CNS';      


end loop;

commit;
end CM_application_API;

procedure call_concurrent_pgm(p_batch_number in varchar2   )
as
l_request_id number;
lv_request_id       NUMBER;
  lc_phase            VARCHAR2(50);
  lc_status           VARCHAR2(50);
  lc_dev_phase        VARCHAR2(50);
  lc_dev_status       VARCHAR2(50);
  lc_message          VARCHAR2(50);
  l_req_return_status BOOLEAN;
  l_cnt  number;
  l_cnt_err number;
begin

select  count(1) into l_cnt  from xx_ar_trx_con_req_tbl where batch_number = p_batch_number;


select  count(1) into l_cnt_err  from xx_ar_transactions_stg where batch_number = p_batch_number and  STATUS_FLAG = 'E' ;


if l_cnt = 0   then
if  l_cnt_err = 0 then

 l_request_id := fnd_request.submit_request ( 
                            application   => 'FND', 
                            program       =>  'XX_AR_INTER_TRANSACTION', 
                            description   =>  'XX AR INTER TRANSACTION', 
                            start_time    =>  sysdate, 
                            sub_request   =>  FALSE,
			    argument1     => p_batch_number
  );
  --
   insert into xx_ar_trx_con_req_tbl (request_id,batch_number ,concurrent_pgm_name )
values
(l_request_id ,P_BATCH_NUMBER ,'XX_AR_INTER_TRANSACTION');
commit;
end if;
end if;

  end call_concurrent_pgm;


PROCEDURE main    ( x_errbuf OUT VARCHAR2, x_retcode OUT NUMBER ,P_BATCH_NUMBER   in VARCHAR2)
AS
l_request_id  number;
BEGIN


xx_ar_iface_transactions.rt_validations(P_BATCH_NUMBER );
xx_ar_iface_transactions.DN_creation_API(P_BATCH_NUMBER) ;
xx_ar_iface_transactions.Receipt_application_API(P_BATCH_NUMBER) ;
--xx_ar_iface_transactions.CM_application_API(P_BATCH_NUMBER) ;



  BEGIN
	  --

       l_request_id := fnd_request.submit_request ( 
                            application   => 'FND', 
                            program       =>  'XXSIFY_AR_DN_CN_APPLICATION', 
                            description   =>  'XXSIFY_AR_DN_CN_APPLICATION', 
                            start_time    =>  sysdate, 
                            sub_request   =>  FALSE,
			    argument1     => p_batch_number
  );


  insert into xx_ar_trx_con_req_tbl (request_id,batch_number ,concurrent_pgm_name )
values
(l_request_id ,P_BATCH_NUMBER ,'XXSIFY_AR_DN_CN_APPLICATION');

		COMMIT;

      EXCEPTION
      WHEN OTHERS THEN
        dbms_output.put_line( 'OTHERS exception while submitting XX_PROGRAM_2: ' || SQLERRM);
      END;

   dbms_output.put_line( ' fnd_conc_global.request_data  ' || fnd_conc_global.request_data);


END;

PROCEDURE DN_CN_APPLICATION    ( x_errbuf OUT VARCHAR2, x_retcode OUT NUMBER ,P_BATCH_NUMBER   in VARCHAR2)
AS

BEGIN 
null;
--fnd_global.apps_initialize (1130 ,    50739 , 222);
xx_ar_iface_transactions.CN_creation_API(P_BATCH_NUMBER) ;

xx_ar_iface_transactions.CM_application_API(P_BATCH_NUMBER) ;
null;

END;


procedure INRR_validations(p_batch_number in varchar2)
as
cursor rt
is
SELECT
rowid,
    ic_type,
    source_entity,
    source_customer_code,
    source_entity_number,
    transfered_entity,
    transfered_customer_code,
    transfered_customer_name,
    transfered_entity_number,
    transfered_entity_date,
    transaction_type,
    currency,
    apply_amount,
    batch_number,
    status_flag,
    error_msg
FROM
    xx_ar_transactions_stg
WHERE
    batch_number =  p_batch_number
    and nvl(status_flag , 'N') = 'N';

l_SOURCE_ENTITY_id  number;
l_status varchar2(20);
l_error_desc  varchar2(2000);
l_CASH_RECEIPT_ID  number;
l_TRANSFERED_ENTITY_ID number;
l_src_CUST_ACCOUNT_ID number;
l_tns_cust_acc_id   number;
l_CUSTOMER_TRX_ID  number;
l_trx_type_id number;
l_currency_code varchar2(200);
l_receipt_amount number;
l_stg_invoice_amt number;
l_inv_amt  number;
l_status_desc  varchar2(200);
l_src_CUSTOMER_TRX_ID number;
 l_src_inv_amt number ;
 l_stg_inv_amt  number;
 l_src_currency_code   varchar2(200);
 l_trn_currency_code varchar2(200);
begin

for i in rt
loop
l_error_desc:= null;
l_status:= 'V';
l_SOURCE_ENTITY_id:= null;
l_CASH_RECEIPT_ID := null;
l_TRANSFERED_ENTITY_ID :=  null;
l_src_CUST_ACCOUNT_ID := null;
l_tns_cust_acc_id := null;
l_CUSTOMER_TRX_ID  := null;
l_trx_type_id :=  null;
l_currency_code := null;
l_receipt_amount := null;
l_stg_invoice_amt:= null;
l_inv_amt:= null;
l_status_desc:=  'Validation Success';
l_src_CUSTOMER_TRX_ID := null;
l_src_inv_amt :=null;
l_stg_inv_amt:=  null;
l_src_currency_code :=  null;
l_trn_currency_code :=  null;
--- source entity
begin
select  ORGANIZATION_ID into l_SOURCE_ENTITY_id from hr_operating_units where SHORT_CODE = i.SOURCE_ENTITY;

exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid SOURCE_ENTITY';
l_status_desc:= 'Validation Error';
end;


---SOURCE_CUSTOMER_CODE
begin

 SELECT CUST_ACCOUNT_ID into l_src_CUST_ACCOUNT_ID    FROM HZ_CUST_ACCOUNTS HCA
   WHERE  hca.account_number = i.SOURCE_CUSTOMER_CODE;

exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid SOURCE_CUSTOMER_CODE';
l_status_desc:= 'Validation Error';
end;


--SOURCE_ENTITY_NUMBER
begin



 SELECT    rcta.CUSTOMER_TRX_ID, nvl(AMOUNT_DUE_REMAINING,0) ,rcta.INVOICE_CURRENCY_CODE 
 into l_src_CUSTOMER_TRX_ID,l_src_inv_amt  , l_src_currency_code
                 FROM ra_customer_trx_all rcta ,
                 ar_payment_schedules_all ps
                WHERE 
                ps.CUSTOMER_TRX_ID = rcta.CUSTOMER_TRX_ID
                and rcta.trx_number = i.SOURCE_ENTITY_NUMBER
                and rcta.org_id = l_SOURCE_ENTITY_id;


exception when others then
       l_status := 'Y' ;
     l_error_desc :=l_error_desc ||' Invalid SOURCE_ENTITY_NUMBER';
     l_status_desc:= 'Validation Error';
end;


--TRANSFERED_ENTITY
begin
select  ORGANIZATION_ID into l_TRANSFERED_ENTITY_ID from hr_operating_units where SHORT_CODE = i.TRANSFERED_ENTITY;
exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid TRANSFERED_ENTITY';
l_status_desc:= 'Validation Error';
end;


--TRANSFERED_CUSTOMER_CODE, TRANSFERED_CUSTOMER_NAME
Begin
SELECT distinct CUST_ACCOUNT_ID into l_tns_cust_acc_id 
    FROM HZ_PARTIES HP,
      HZ_CUST_ACCOUNTS HCA
    WHERE HP.PARTY_ID = HCA.PARTY_ID
--    AND HP.PARTY_NAME = i.TRANSFERED_CUSTOMER_NAME
    and hca.account_number = i.TRANSFERED_CUSTOMER_CODE;

exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid TRANSFERED_CUSTOMER_CODE, TRANSFERED_CUSTOMER_NAME ';
l_status_desc:= 'Validation Error';
end;




begin



   SELECT    rcta.CUSTOMER_TRX_ID, nvl(AMOUNT_DUE_REMAINING,0), rcta.INVOICE_CURRENCY_CODE  
                into l_CUSTOMER_TRX_ID,l_inv_amt, l_trn_currency_code
                 FROM ra_customer_trx_all rcta ,
                 ar_payment_schedules_all ps
                WHERE 
                ps.CUSTOMER_TRX_ID = rcta.CUSTOMER_TRX_ID
                and rcta.trx_number = i.TRANSFERED_ENTITY_NUMBER
                and rcta.org_id = l_TRANSFERED_ENTITY_ID;



exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid TRANSFERED_ENTITY_NUMBER' ;
l_status_desc:= 'Validation Error';
end;

dbms_output.put_line(l_inv_amt || i.APPLY_AMOUNT );
--if l_inv_amt < i.APPLY_AMOUNT then
--l_status:= 'E';
--l_error_desc :=l_error_desc || ' APPLY AMOUNT should be less than or equal to invoice amount  ' ;
--l_status_desc:= 'Validation Error';
--end if;

---- invoice amount check 

select sum(APPLY_AMOUNT) into l_stg_inv_amt from XX_AR_TRANSACTIONS_STG
where BATCH_NUMBER = i.BATCH_NUMBER 
and TRANSFERED_ENTITY_NUMBER = i.TRANSFERED_ENTITY_NUMBER;



dbms_output.put_line(l_inv_amt || i.APPLY_AMOUNT );
if l_inv_amt < l_stg_inv_amt then
l_status:= 'E';
l_error_desc :=l_error_desc || ' APPLY AMOUNT should be less than or equal to invoice amount of transfered entity  ' ;
l_status_desc:= 'Validation Error';
end if;




select sum(APPLY_AMOUNT) into l_stg_inv_amt from XX_AR_TRANSACTIONS_STG
where BATCH_NUMBER = i.BATCH_NUMBER 
and TRANSFERED_ENTITY_NUMBER = i.SOURCE_ENTITY_NUMBER;



dbms_output.put_line(l_src_inv_amt || i.APPLY_AMOUNT );
if l_src_inv_amt < l_stg_inv_amt then
l_status:= 'E';
l_error_desc :=l_error_desc || ' APPLY AMOUNT should be less than or equal to invoice amount of Source entity  ' ;
l_status_desc:= 'Validation Error';
end if;



if i.APPLY_AMOUNT  <= 0  then
l_status:= 'E';
l_error_desc :=l_error_desc || ' APPLY AMOUNT should be greater than 0  ' ;
l_status_desc:= 'Validation Error';
end if;


--
--begin
--
--SELECT cust_trx_type_id    into l_trx_type_id              
--                 FROM ra_cust_trx_types_all
--                WHERE name = i.TRANSACTION_TYPE;
--
--exception when others then
--l_status:= 'E';
--l_error_desc :=l_error_desc || ' Invalid TRANSACTION_TYPE' ;
--l_status_desc:= 'Validation Error';
--end ;


Begin
 SELECT currency_code
              INTO l_currency_code
              FROM fnd_currencies
              WHERE currency_code =  i.CURRENCY;

exception when others then
l_status:= 'E';
l_error_desc :=l_error_desc || ' Invalid CURRENCY ' ;
l_status_desc:= 'Validation Error';
end ;


if l_trn_currency_code =  i.CURRENCY then
null;
else

l_status:= 'E';
l_error_desc :=l_error_desc || ' Given  currency code differs from currency code of Transfered Entity Number ' ;
l_status_desc:= 'Validation Error';
end if ;



if l_src_currency_code =  i.CURRENCY then
null;
else

l_status:= 'E';
l_error_desc :=l_error_desc || ' Given  currency code differs from currency code of Source Entity Number ' ;
l_status_desc:= 'Validation Error';
end if ;



--- conversion rate  for source entity
--
--
--SELECT
--    closing_status into l_closing_status 
--FROM
--    gl_period_statuses
--WHERE
--    application_id = 222
--    AND set_of_books_id = ( SELECT
--    set_of_books_id
--FROM
--    hr_operating_units where organization_id =l_SOURCE_ENTITY_id )
--AND SYSDATE BETWEEN start_date AND end_date;
--
--
--
--
--if l_closing_status = 'O'  then
--
--l_date := sysdate;
--else
--begin
--SELECT
--   max(END_DATE)  into l_date
--FROM
--    gl_period_statuses
--WHERE
--    application_id = 222
--    AND set_of_books_id = ( SELECT
--    set_of_books_id
--FROM
--    hr_operating_units where organization_id = l_SOURCE_ENTITY_id)
--and closing_status = 'O';
--
--exception when others then
--l_date := sysdate;
--end;
--
--end if; 
--
--if i.currency  = 'INR' 
--then
--
--null;
--
--else
--
--
--begin
--
--select count(1) into l_conv_rate_cnt   from 
--gl_daily_rates
--where 
--FROM_CURRENCY= i.currency
--and TO_CURRENCY = 'INR'
-- and to_date(CONVERSION_DATE) = to_date(l_date);
--
--
--end;
--
--
--end if;
--
--
--
--if l_conv_rate_cnt = 0 then
--l_status:= 'E';
--l_error_desc :=l_error_desc || ' Currency rate has not yet defined for in source entity ';
--l_status_desc:= 'Validation Error';
--end if;
--
--
--
-------
--
----- conversion rate  for Transfered entity
--
--l_closing_status:=  null;
--l_date:=  null;
--l_conv_rate_cnt:=  null;
--
--SELECT
--    closing_status into l_closing_status 
--FROM
--    gl_period_statuses
--WHERE
--    application_id = 222
--    AND set_of_books_id = ( SELECT
--    set_of_books_id
--FROM
--    hr_operating_units where organization_id =l_TRANSFERED_ENTITY_ID )
--AND SYSDATE BETWEEN start_date AND end_date;
--
--
--
--
--if l_closing_status = 'O'  then
--
--l_date := sysdate;
--else
--begin
--SELECT
--   max(END_DATE)  into l_date
--FROM
--    gl_period_statuses
--WHERE
--    application_id = 222
--    AND set_of_books_id = ( SELECT
--    set_of_books_id
--FROM
--    hr_operating_units where organization_id = l_TRANSFERED_ENTITY_ID)
--and closing_status = 'O';
--
--exception when others then
--l_date := sysdate;
--end;
--
--end if; 
--
--if i.currency  = 'INR' 
--then
--
--null;
--
--else
--
--
--begin
--
--select count(1) into l_conv_rate_cnt   from 
--gl_daily_rates
--where 
--FROM_CURRENCY= i.currency
--and TO_CURRENCY = 'INR'
-- and to_date(CONVERSION_DATE) = to_date(l_date);
--
--
--end;
--
--
--end if;

--
--
--if l_conv_rate_cnt = 0 then
--l_status:= 'E';
--l_error_desc :=l_error_desc || ' Currency rate has not yet defined for in transfered entity ';
--l_status_desc:= 'Validation Error';
--end if;
--

 --- transfered_entity and source_entity
 
if i.transfered_entity = i.source_entity then

l_status:= 'E';
l_error_desc :=l_error_desc || ' transfered_entity and source_entity should not be same' ;
l_status_desc:= 'Validation Error';

end if;



update xx_ar_transactions_stg 
set 
status_flag = l_status,
status_desc = l_status_desc,
error_msg = l_error_desc,
source_OU_ID = l_SOURCE_ENTITY_id,
SOURCE_CUST_ACC_ID=l_src_CUST_ACCOUNT_ID
,SOURCE_ENTITY_ID=l_CASH_RECEIPT_ID
, TRANSFERED_OU_ID  = l_TRANSFERED_ENTITY_ID
, TRANSFERED_CUST_ACC_ID=l_tns_cust_acc_id
, TRANSFERED_ENTITY_ID= l_CUSTOMER_TRX_ID
, CUST_TRX_TYPE_ID=l_trx_type_id

where rowid = i.rowid;

end loop;
commit;
end INRR_validations;

procedure INRR_DN_creation_API(p_batch_number in varchar2)
as
begin

null ;
end INRR_DN_creation_API;


procedure INRR_CN_creation_API(p_batch_number in varchar2)
as

cursor c1 is

SELECT
    SUM(apply_amount) apply_amount,
    batch_number,
    ic_type,
    source_entity,
    source_customer_code,
    source_entity_number,
    transfered_entity,
    source_entity_id,
    source_cust_acc_id,
    source_ou_id
FROM
    xx_ar_transactions_stg
WHERE
    batch_number = p_batch_number
    AND status_flag = 'V'
GROUP BY
    ic_type,
    batch_number,
    source_entity,
    source_customer_code,
    source_entity_number,
    transfered_entity,
    source_entity_id,
    source_cust_acc_id,
    source_ou_id;

 v_memo_line_id          NUMBER          DEFAULT NULL;
v_trx_header_id         NUMBER          DEFAULT NULL;
l_customer_trx_id       NUMBER;
o_return_status         VARCHAR2(1);
o_msg_count             NUMBER;
o_msg_data              VARCHAR2(2000);
 l_cnt                   NUMBER          DEFAULT 0;
l_msg_index_out         NUMBER;
l_trx_number            VARCHAR2(100);
l_status varchar2(20);
l_error_desc  varchar2(2000);
v_batch_source_id       ra_batch_sources_all.batch_source_id%TYPE DEFAULT NULL;
l_batch_source_rec      ar_invoice_api_pub.batch_source_rec_type;
l_memo_line_name  varchar2(200);
 l_cust_trx_type_id    ra_customer_trx_all.CUST_TRX_TYPE_ID%TYPE DEFAULT NULL;
l_trx_header_tbl        ar_invoice_api_pub.trx_header_tbl_type;
l_trx_lines_tbl         ar_invoice_api_pub.trx_line_tbl_type;
l_trx_dist_tbl          ar_invoice_api_pub.trx_dist_tbl_type;
l_trx_salescredits_tbl  ar_invoice_api_pub.trx_salescredits_tbl_type;
l_status_desc varchar2(200);
l_closing_status  varchar2(200);
l_date date;
begin

for i in c1 
loop

delete from ar_trx_errors_gt;
commit;
l_status :='DNS';
l_error_desc  := null;
l_status_desc:= 'Success';
l_closing_status :=  null;
l_date := null;
   BEGIN

        SELECT ra_customer_trx_s.nextval 
          INTO v_trx_header_id 
          FROM dual;

        EXCEPTION WHEN OTHERS THEN
fnd_file.put_line(fnd_file.LOG,  '  error while getting the ra_customer_trx_s sequence '); 

        l_status := 'DNE';
        l_error_desc :=l_error_desc || ' error while getting the ra_customer_trx_s sequence ' ;
        l_status_desc:= 'Debit Note Error';
        END ;

begin
select BATCH_SOURCE_ID into v_batch_source_id
from ra_batch_sources_all
where upper(name) like upper('IC-RT')
and org_id =i.SOURCE_OU_ID ;
exception when others then
fnd_file.put_line(fnd_file.LOG,  ' error while getting the batch source '); 
 l_status_desc:= 'Debit Note Error';
l_status:= 'DNE';
l_error_desc :=l_error_desc || ' error while getting the batch source ' ;
end;



--v_batch_source_id :=13003;


begin

select MEANING into l_memo_line_name 
from fnd_lookup_values 
where lookup_type = 'XX_AR_TRANS_MEMO_LINE' 
and DESCRIPTION = i.source_entity
and tag = i.TRANSFERED_ENTITY;


SELECT    t.memo_line_id into v_memo_line_id
  FROM ar_memo_lines_all_tl t
      ,ar_memo_lines_all_b b
 WHERE  b.memo_line_id = t.memo_line_id
   AND  t.name =   l_memo_line_name
   and b.org_id  =i.source_ou_id ;

exception when others then

fnd_file.put_line(fnd_file.LOG,  '  error while getting the  MEMO LINE NAME'); 
 l_status_desc:= 'Debit Note Error';
l_status:= 'DNE';
l_error_desc :=l_error_desc || ' error while getting the  MEMO LINE NAME ' ;

end;





begin

 select rctta.CUST_TRX_TYPE_ID  into l_cust_trx_type_id from 
      gl_code_combinations_kfv gcc, ra_cust_trx_types_all rctta
 where
rctta.GL_ID_REV =  gcc.code_combination_id and   rctta.type ='DM'
and gcc.segment2  in (
 select gcc.segment2  from ar_cash_receipts_all rcra ,
                AR_RECEIPT_METHOD_ACCOUNTS_ALL rmaa,
                gl_code_combinations_kfv gcc
                where 
                rcra.RECEIPT_METHOD_ID = rmaa.RECEIPT_METHOD_ID
                and receipt_number = i.source_entity_number
                and rcra.org_id = i.source_ou_id
                and rmaa.CASH_CCID = gcc.code_combination_id
) and rownum = 1   and rctta.org_id= i.source_ou_id   and  rctta.NAME  like  '%-DM';


--l_cust_trx_type_id:= 17004;
dbms_output.put_line('l_cust_trx_type_id   '||l_cust_trx_type_id);

end ;




SELECT
    closing_status into l_closing_status 
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id =i.SOURCE_OU_ID )
AND SYSDATE BETWEEN start_date AND end_date;




if l_closing_status = 'O'  then

l_date := sysdate;
else
begin
SELECT
   max(END_DATE)  into l_date
FROM
    gl_period_statuses
WHERE
    application_id = 222
    AND set_of_books_id = ( SELECT
    set_of_books_id
FROM
    hr_operating_units where organization_id = i.SOURCE_OU_ID)
and closing_status = 'O';

exception when others then
l_date := sysdate;
end;

end if; 





            l_trx_header_tbl (1).trx_header_id                  := v_trx_header_id;
            l_trx_header_tbl (1).trx_number                     := NULL;
            l_trx_header_tbl (1).bill_to_customer_id            := i.SOURCE_CUST_ACC_ID;
            l_trx_header_tbl (1).cust_trx_type_id               := l_cust_trx_type_id;
              l_trx_header_tbl (1).trx_date               := l_date;
                l_trx_header_tbl (1).gl_date               := l_date;
            l_trx_header_tbl (1).comments                       := 'RT DN Creation';
            l_batch_source_rec.batch_source_id                  := v_batch_source_id;
              l_trx_header_tbl (1).interface_header_attribute15   := 'TN - CHENNAI:Bill To';
            l_trx_lines_tbl (1).trx_header_id                   := v_trx_header_id;
            l_trx_lines_tbl (1).trx_line_id                     := ra_customer_trx_lines_s.nextval;
            l_trx_lines_tbl (1).line_number                     := 1;
            l_trx_lines_tbl (1).description                     := 'RT DN Creation';
            l_trx_lines_tbl (1).memo_line_id                    := v_memo_line_id;
            l_trx_lines_tbl (1).quantity_invoiced               := 1;
            l_trx_lines_tbl (1).unit_selling_price              := i.APPLY_AMOUNT;
            l_trx_lines_tbl (1).line_type                       := 'LINE';

fnd_file.put_line(fnd_file.LOG, '--------------------APPS INITIALIZATION--------------------');
mo_global.set_policy_context ('S',i.SOURCE_OU_ID );
fnd_file.put_line(fnd_file.LOG, '------------------------API  STARTS------------------------');

if l_status = 'DNS' then
   ar_invoice_api_pub.create_single_invoice ( -- std parameters
                                            p_api_version            => 1.0
                                           ,p_init_msg_list          => fnd_api.g_false
                                           ,p_commit                 => fnd_api.g_false
                                           -- api parameters
                                           ,p_batch_source_rec       => l_batch_source_rec
                                           ,p_trx_header_tbl         => l_trx_header_tbl
                                           ,p_trx_lines_tbl          => l_trx_lines_tbl
                                           ,p_trx_dist_tbl           => l_trx_dist_tbl
                                           ,p_trx_salescredits_tbl   => l_trx_salescredits_tbl
                                           -- Out parameters
                                           ,x_customer_trx_id        => l_customer_trx_id
                                           ,x_return_status          => o_return_status
                                           ,x_msg_count              => o_msg_count
                                           ,x_msg_data               => o_msg_data);



fnd_file.put_line(fnd_file.LOG, 'API RETURN STATUS          - '||o_return_status);                                           
fnd_file.put_line(fnd_file.LOG, '------------------------API  ENDS--------------------------'); 
commit;
 IF o_return_status = fnd_api.g_ret_sts_error
        OR o_return_status = fnd_api.g_ret_sts_unexp_error THEN
 l_error_desc := o_msg_data  ;

        IF o_msg_count > 0 THEN
fnd_file.put_line(fnd_file.LOG, 'API MESSAGE COUNT   		- '||o_msg_count);
            FOR v_index IN 1 .. o_msg_count
            LOOP
            fnd_msg_pub.get (p_msg_index       => v_index
                            ,p_encoded         => 'F'
                            ,p_data            => o_msg_data
                            ,p_msg_index_out   => l_msg_index_out);
            o_msg_data := substr (o_msg_data, 1, 3950);



l_status:= 'DNE';
l_error_desc :=l_error_desc || o_msg_data;
 l_status_desc:= 'Debit Note Error';
--
--            update xx_ar_transactions_stg set 
--            STATUS_FLAG = 'DNE', ERROR_MSG = o_msg_data
--            where batch_number = i.batch_number
--            and Source_entity_number = i.source_entity_number
--            AND status_flag = 'V';

fnd_file.put_line(fnd_file.LOG, 'ERROR MESSAGE       	- '||o_msg_data);

            END LOOP;

        END IF;

      ELSE

        SELECT count (*) INTO l_cnt FROM ar_trx_errors_gt;   

        IF l_cnt = 0 THEN

          BEGIN
            SELECT trx_number
            INTO   l_trx_number
            FROM   ra_customer_trx_all
            WHERE  customer_trx_id = l_customer_trx_id;
          END;


          insert into  xx_ar_transactions_dtl
          (
          batch_number,
          SOURCE_ENTITY_NUMBER,
          SOURCE_ENTITY,
          TRANSFERED_ENTITY,
          DN_TRX_NUMBER,
          LAST_UPDATE_DATE, LAST_UPDATED_BY, LAST_UPDATE_LOGIN, CREATED_BY,CREATION_DATE
          )
          values
          (
          i.batch_number,
          i.SOURCE_ENTITY_NUMBER,
          i.SOURCE_ENTITY,
          i.TRANSFERED_ENTITY,
          l_trx_number,
          sysdate,
          fnd_global.user_id,
          fnd_global.user_id,
          fnd_global.user_id
          ,sysdate
          );

        ELSE 



l_status:= 'DNE';
l_error_desc :=l_error_desc || 'Transaction not Created, Please check Oracle ar_trx_errors_gt table trx_header_id '||v_trx_header_id;
 l_status_desc:= 'Debit Note Error';
--            update xx_ar_transactions_stg set 
--            STATUS_FLAG = 'DNE', ERROR_MSG = 'Transaction not Created, Please check Oracle ar_trx_errors_gt table trx_header_id '||v_trx_header_id
--            where batch_number = i.batch_number
--            and Source_entity_number = i.source_entity_number
--            AND status_flag = 'V';
fnd_file.put_line(fnd_file.LOG, 'Transaction not Created, Please check Oracle ar_trx_errors_gt table  trx_header_id'||v_trx_header_id);

--        BEGIN
--            
--            SELECT LISTAGG(error_message, ',') WITHIN GROUP (ORDER BY error_message) Error_message
--              INTO l_error_message 
--              FROM ar_trx_errors_gt
--             WHERE trx_header_id = v_trx_header_id
--            GROUP BY  trx_header_id;
--              
--        EXCEPTION WHEN OTHERS THEN 
--        
--        l_error_message := NULL;
--         
--        END ;
--        


        END IF;





    END IF; 

    end if;

            update xx_ar_transactions_stg set 
            STATUS_FLAG = l_status,
             DN_STATUS =l_status_desc,
            ERROR_MSG = l_error_desc
            where batch_number = i.batch_number
            and Source_entity_number = i.source_entity_number
            AND status_flag = 'V';    

end loop;
EXCEPTION WHEN OTHERS THEN 
fnd_file.put_line(fnd_file.LOG, ' EXCEPTION occurs '||SQLERRM);

end INRR_CN_creation_API;

procedure INRR_CM_application_API(p_batch_number in varchar2)
as
begin

null ;
end INRR_CM_application_API;


end xx_ar_iface_transactions;