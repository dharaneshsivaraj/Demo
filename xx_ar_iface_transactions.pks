create or replace package xx_ar_iface_transactions
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
);

procedure get_batch_number(p_batch_number out number);

procedure delete_batch (p_batch_number in varchar2);

procedure rt_validations(p_batch_number in varchar2);

procedure DN_creation_API(p_batch_number in varchar2);

procedure CN_creation_API(p_batch_number in varchar2);

procedure Receipt_application_API(p_batch_number in varchar2);

procedure call_concurrent_pgm(p_batch_number in varchar2  );
procedure CM_application_API(p_batch_number in varchar2);

PROCEDURE main    ( x_errbuf OUT VARCHAR2, x_retcode OUT NUMBER ,P_BATCH_NUMBER   in VARCHAR2
                               );

PROCEDURE DN_CN_APPLICATION    ( x_errbuf OUT VARCHAR2, x_retcode OUT NUMBER ,P_BATCH_NUMBER   in VARCHAR2
                               );

procedure INRR_validations(p_batch_number in varchar2);

procedure INRR_DN_creation_API(p_batch_number in varchar2);

procedure INRR_CN_creation_API(p_batch_number in varchar2);

procedure INRR_CM_application_API(p_batch_number in varchar2);





end xx_ar_iface_transactions;