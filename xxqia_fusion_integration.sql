CREATE OR REPLACE package body APPS.xxqia_fusion_integration
--AUTHID CURRENT_USER

as

procedure process_grades_dat(p_file_name in VARCHAR2)
as
cursor c1
is

SELECT
    'METADATA' as METADATA,
    'Grade' as Grade,
    'SourceSystemOwner' as SourceSystemOwner,
    'SourceSystemId' as SourceSystemId,
    'EffectiveStartDate' as EffectiveStartDate,
    'EffectiveEndDate' as EffectiveEndDate,
    'SetCode'   as SetCode,
    'GradeCode' as GradeCode,
    'GradeName' as GradeName,
    'ActiveStatus' as ActiveStatus
FROM
    dual
UNION ALL
SELECT DISTINCT
    'MERGE' as METADATA,
    'Grade' as Grade,
    'QIA_EBS' as SourceSystemOwner,
    to_char(grade_id)   as SourceSystemId,
    to_char(date_from, 'YYYY/MM/DD') as EffectiveStartDate,
    to_char(date_to, 'YYYY/MM/DD') as EffectiveEndDate,
    'QIA' as SetCode,
    to_char(grade_id) as GradeCode,
    name as GradeName,
    CASE
        WHEN ( date_to IS NULL
               OR date_to > sysdate ) THEN
            'A'
        ELSE
            'I'
    END  as ActiveStatus
FROM
    per_grades where trunc(CREATION_DATE) = trunc(sysdate);


    l_file_name varchar2(200);
    lv_check_file_exist      boolean;
    l_global_file            varchar2 (100);
          lv_a                     number;
      lv_b                     number;
        l_dir_name               varchar2 (100);
        l_file_handle            UTL_FILE.file_type;

begin
      DBMS_OUTPUT.enable (200000000000);

l_file_name := p_file_name ||'.dat';
l_dir_name := '/usr/tmp';
  UTL_FILE.fgetattr (l_dir_name,
                         l_file_name,
                         lv_check_file_exist,
                         lv_a,
                         lv_b);

     fnd_file.put_line (fnd_file.LOG,       'Note: File will be generated in this path: ' || l_dir_name     );                         
--l_global_file := l_file_name;
      l_file_handle :=
        UTL_FILE.fopen (l_dir_name,
                         l_file_name,
                         'W',
                         32767);
FOR i IN c1
      LOOP
         UTL_FILE.put_line (
            l_file_handle,
    i.metadata  || '|' 
   || i.grade    || '|' 
   || i.sourcesystemowner    || '|' 
   || i.sourcesystemid    || '|' 
   || i.effectivestartdate    || '|' 
   || i.effectiveenddate    || '|' 
   || i.setcode    || '|' 
   || i.gradecode    || '|' 
   || i.gradename    || '|' 
   || i.activestatus 
         );


        end loop;

UTL_FILE.fclose(l_file_handle);




EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         dbms_output.put_line ( 
                            'Invalid Operation For ' || l_global_file); 
         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_path
      THEN
         dbms_output.put_line ( 
                            'Invalid Path For   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_mode
      THEN
         dbms_output.put_line ( 
                            'Invalid Mode For   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_filehandle
      THEN
         dbms_output.put_line ( 
                            'Invalid File Handle  ' || l_global_file);
         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.write_error
      THEN
         dbms_output.put_line (
                            'Invalid Write Error   ' || l_global_file);

        UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.read_error
      THEN
         dbms_output.put_line (
                            'Invalid Read  Error   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.internal_error
      THEN
         dbms_output.put_line (  'Internal Error');
         UTL_FILE.fclose(l_file_handle);
      WHEN OTHERS
      THEN
         dbms_output.put_line ( 'Error message due to others is: ' || SQLERRM);
         UTL_FILE.fclose(l_file_handle);

end process_grades_dat;

procedure process_Positions_dat(p_file_name in VARCHAR2)
as

    l_file_name varchar2(200);
    lv_check_file_exist      boolean;
    l_global_file            varchar2 (100);
          lv_a                     number;
      lv_b                     number;
        l_dir_name               varchar2 (100);
        l_file_handle            UTL_FILE.file_type;
        l_file_handle_1            UTL_FILE.file_type;
                l_file_handle_2            UTL_FILE.file_type;

        cursor c1 is
SELECT
    'METADATA' as metadata,
    'Job' as JOB,
    'SourceSystemOwner' as SourceSystemOwner ,
    'SourceSystemId' as SourceSystemId,
    'EffectiveStartDate' as EffectiveStartDate,
    'EffectiveEndDate' as EffectiveEndDate ,
    'SetCode' as SetCode,
    'JobCode' as JobCode,
    'Name' as Name,
    'ActiveStatus' as ActiveStatus,
    'RegularTemporary' as RegularTemporary
FROM
    dual
UNION ALL
SELECT
    'MERGE' as metadata,
    'Job' as JOB,
    'QIA_EBS' as SourceSystemOwner,
    to_char(pp.position_id) as SourceSystemId,
    to_char(pp.date_effective, 'YYYY/MM/DD') as EffectiveStartDate,
    to_char(pp.date_end, 'YYYY/MM/DD')  as EffectiveEndDate ,
    'QIA' as SetCode,
    to_char(pp.position_id) as JobCode,
    pp.name as Name,
    CASE
        WHEN ( pp.date_end IS NULL
               OR pp.date_end > sysdate ) THEN
            'A'
        ELSE
            'I'
    END AS   ActiveStatus,
    'R' as RegularTemporary
FROM
    per_positions              pp,
    per_position_definitions   ppd
WHERE
    pp.position_definition_id = ppd.position_definition_id
    and   trunc(ppd.CREATION_DATE) = trunc(sysdate);

begin

DBMS_OUTPUT.enable (200000000000);

l_file_name :=  p_file_name ||'.dat';
l_dir_name := '/usr/tmp';
  UTL_FILE.fgetattr (l_dir_name,
                         l_file_name,
                         lv_check_file_exist,
                         lv_a,
                         lv_b);

     fnd_file.put_line (fnd_file.LOG,       'Note: File will be generated in this path: ' || l_dir_name     );                         
--l_global_file := l_file_name;
      l_file_handle :=
        UTL_FILE.fopen (l_dir_name,
                         l_file_name,
                         'W',
                         32767);
FOR i IN c1
      LOOP
         UTL_FILE.put_line (
            l_file_handle,
          i.METADATA           
|| '|'  || i.JOB
|| '|'  || i.SOURCESYSTEMOWNER
|| '|'  || i.SOURCESYSTEMID
|| '|'  || i.EFFECTIVESTARTDATE
|| '|'  || i.EFFECTIVEENDDATE
|| '|'  || i.SETCODE
|| '|'  || i.JOBCODE
|| '|'  || i.NAME
|| '|'  || i.ACTIVESTATUS
|| '|'  || i.REGULARTEMPORARY
         );


        end loop;

UTL_FILE.fclose(l_file_handle);


-- UTL_FILE.fclose(l_file_handle);

EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         dbms_output.put_line ( 
                            'Invalid Operation For ' || l_global_file); 
         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_path
      THEN
         dbms_output.put_line ( 
                            'Invalid Path For   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_mode
      THEN
         dbms_output.put_line ( 
                            'Invalid Mode For   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_filehandle
      THEN
         dbms_output.put_line ( 
                            'Invalid File Handle  ' || l_global_file);
         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.write_error
      THEN
         dbms_output.put_line (
                            'Invalid Write Error   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.read_error
      THEN
         dbms_output.put_line (
                            'Invalid Read  Error   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.internal_error
      THEN
         dbms_output.put_line (  'Internal Error');
         UTL_FILE.fclose(l_file_handle);
      WHEN OTHERS
      THEN
         dbms_output.put_line ( 'Error message due to others is: ' || SQLERRM);
        UTL_FILE.fclose(l_file_handle);

end  process_Positions_dat;

procedure process_Org_dat(p_file_name in VARCHAR2)
as
cursor c1
is
SELECT
    'METADATA' as metadata,
    'Organization' as Organization,
    'SourceSystemOwner' as SourceSystemOwner ,
    'SourceSystemId' as SourceSystemId,
    'EffectiveStartDate' as EffectiveStartDate,
    'EffectiveEndDate' as EffectiveEndDate ,
    'Name' as name
FROM
    dual

UNION ALL
SELECT DISTINCT
    'MERGE' as METADATA,
    'Organization' as Organization,
    'QIA_EBS' as SourceSystemOwner,
   to_char(hou.organization_id) as SourceSystemId,
    to_char(hou.date_from, 'YYYY/MM/DD')  as EffectiveStartDate,
   to_char (nvl(hou.date_to,'31-DEC-4712'), 'YYYY/MM/DD') as EffectiveEndDate,
    hou.name as name
FROM
        hr_organization_units     hou,
    hr_organization_information   hoi
WHERE
    hou.organization_id = hoi.organization_id
    and      hou.TYPE = '20'
--          and hou.organization_id =8338;

   and     trunc( hoi.CREATION_DATE) = trunc(sysdate)     ;

--

cursor c2
is

SELECT
    'METADATA' as metadata,
    'OrgUnitClassification' as OrgUnitClassification,
    'SourceSystemOwner' as SourceSystemOwner ,
    'SourceSystemId' as SourceSystemId ,
    'EffectiveStartDate' as EffectiveStartDate,
    'EffectiveEndDate' as EffectiveEndDate,
    'OrganizationId(SourceSystemId)' as Organizationid,
    'ClassificationCode' ClassificationCode,
   'SetCode' as SetCode
    ,'Status' as Status
FROM
    dual
UNION ALL
SELECT 
DISTINCT
    'MERGE' as METADATA,
    'OrgUnitClassification'  as OrgunitClassification,
    'QIA_EBS'  as SourceSystemOwner,
    to_char( hoi.org_information_id)  as SourceSystemId,
    to_char(hou.date_from, 'YYYY/MM/DD')   as EffectiveStartDate,
  to_char (nvl(hou.date_to,'31-DEC-4712'), 'YYYY/MM/DD') as EffectiveendDate,
    to_char(hou.organization_id)  as Organizationid,
    'DEPARTMENT' as ClassificationCode,
    'QIA' as SetCode,
    CASE
        WHEN ( hou.date_to IS NULL
               OR hou.date_to > sysdate ) THEN
            'A'
        ELSE
            'I'
    END AS status

FROM
    hr_organization_units     hou,
    hr_organization_information   hoi
WHERE

    hou.organization_id = hoi.organization_id
        and      hou.TYPE = '20'
    and     trunc( hoi.CREATION_DATE) = trunc(sysdate)
--      and hou.organization_id =8338
      ;
--    and hou.organization_id in
--                ( select organization_id FROM
--                hr_all_organization_units where organization_id in (8336))
--                ;

    l_file_name varchar2(200);
    lv_check_file_exist      boolean;
    l_global_file            varchar2 (100);
          lv_a                     number;
      lv_b                     number;
        l_dir_name               varchar2 (100);
        l_file_handle            UTL_FILE.file_type;
        l_file_handle_1    UTL_FILE.file_type;
        l_cnt number;

begin
      DBMS_OUTPUT.enable (200000000000);
   
              fnd_file.put_line (fnd_file.LOG,       'after c2'     );   

--
--

l_file_name := p_file_name ||'.dat';
l_dir_name := '/usr/tmp';
  UTL_FILE.fgetattr (l_dir_name,
                         l_file_name,
                         lv_check_file_exist,
                         lv_a,
                         lv_b);
                         
     fnd_file.put_line (fnd_file.LOG,       ' fgetattr '     );                         

     fnd_file.put_line (fnd_file.LOG,       'Note: File will be generated in this path: ' || l_dir_name     );                         
--l_global_file := l_file_name;
      l_file_handle_1 :=
         UTL_FILE.fopen (l_dir_name,
                         l_file_name,
                         'W',32767);

     fnd_file.put_line (fnd_file.LOG,       ' fopen '     ); 
     
     
     
l_cnt:=0;
FOR i IN c1
      LOOP
      
      
           fnd_file.put_line (fnd_file.LOG,       ' c1 '     );                         

          dbms_output.put_line ('Insied c1'  ); 
                            
         UTL_FILE.put_line (
            l_file_handle_1,
    i.metadata  || '|' 
   || i.ORGANIZATION    || '|' 
   || i.SOURCESYSTEMOWNER    || '|' 
   || i.SOURCESYSTEMID    || '|' 
   || i.EFFECTIVESTARTDATE    || '|' 
   || i.EFFECTIVEENDDATE    || '|' 
   || i.NAME  
         );
l_cnt := l_cnt +1;
        end loop;

      ----org classification  header
       ----org classification  data
       
       l_cnt:= 0 ;
        for j in c2
        loop
                   fnd_file.put_line (fnd_file.LOG,       ' c2 '     );     
             dbms_output.put_line ('Insied c2'  ); 
           UTL_FILE.put_line (
            l_file_handle_1,
    j.metadata  || '|' 
   || j.OrgunitClassification    || '|' 
   || j.SOURCESYSTEMOWNER    || '|' 
   || j.SOURCESYSTEMID    || '|' 
   || j.EFFECTIVESTARTDATE    || '|' 
   || j.EFFECTIVEENDDATE    || '|' 
   || j.Organizationid    || '|' 
   || j.ClassificationCode  || '|' 
   || j.SetCode  || '|' 
   || j.status 
         );
l_cnt := l_cnt +1;
        end loop;
        
                  dbms_output.put_line ('Insied end loop'  ); 
           fnd_file.put_line (fnd_file.LOG,       '  end loop '     );                         

-- UTL_FILE.FFLUSH(l_file_handle_1);
            fnd_file.put_line (fnd_file.LOG,       '  end FFLUSH '     );                         

           dbms_output.put_line ('Insied FFLUSH'  ); 

UTL_FILE.fclose(l_file_handle_1);
             fnd_file.put_line (fnd_file.LOG,       '  end fclose '     );                         

           dbms_output.put_line ('Insied fclose'  ); 



EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         dbms_output.put_line ( 
                            'Invalid Operation For ' || l_global_file); 
                            
                                               fnd_file.put_line (fnd_file.LOG,     'Invalid Operation For ' || l_global_file   );                         

         UTL_FILE.fclose(l_file_handle_1);
      WHEN UTL_FILE.invalid_path
      THEN
         dbms_output.put_line ( 
                            'Invalid Path For   ' || l_global_file);
                                               fnd_file.put_line (fnd_file.LOG,'Invalid Path For   ' || l_global_file );                         

         UTL_FILE.fclose(l_file_handle_1);
      WHEN UTL_FILE.invalid_mode
      THEN
         dbms_output.put_line ( 
                            'Invalid Mode For   ' || l_global_file);
                                               fnd_file.put_line (fnd_file.LOG,'Invalid Mode For   ' || l_global_file );                         
                            

         UTL_FILE.fclose(l_file_handle_1);
      WHEN UTL_FILE.invalid_filehandle
      THEN
                                               fnd_file.put_line (fnd_file.LOG,' File Handle For   ' || l_global_file );                         
                            
         dbms_output.put_line ( 
                            'Invalid File Handle  ' || l_global_file);
         UTL_FILE.fclose(l_file_handle_1);
      WHEN UTL_FILE.write_error
      THEN
                                                     fnd_file.put_line (fnd_file.LOG,' File Write For   ' || l_global_file );                         

         dbms_output.put_line (
                            'Invalid Write Error   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle_1);
      WHEN UTL_FILE.read_error
      THEN
      fnd_file.put_line (fnd_file.LOG,' File Read For   ' || l_global_file );                         

         dbms_output.put_line (
                            'Invalid Read  Error   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle_1);
      WHEN UTL_FILE.internal_error
      THEN
         dbms_output.put_line (  'Internal Error');
fnd_file.put_line (fnd_file.LOG,' Internal Error   ' || l_global_file );                                  
         UTL_FILE.fclose(l_file_handle_1);
      WHEN OTHERS
      THEN
      fnd_file.put_line (fnd_file.LOG,'Error message due to others is: '|| SQLERRM );                                  

         dbms_output.put_line ( 'Error message due to others is: ' || SQLERRM);
--         UTL_FILE.fclose(l_file_handle_1);

end process_Org_dat;

procedure process_Worker_dat(p_file_name in VARCHAR2)
as
cursor c_Worker
is
SELECT  distinct
    'MERGE' AS merge,
    'Worker' AS worker,
    papf.employee_number              AS person_id,
   to_char(papf.effective_start_date, 'YYYY/MM/DD')     AS effective_start_date,
   ''       AS effective_end_date,
    papf.employee_number        AS employee_number,
    '' bloodtype,
    '' correspondencelanguage,
     to_char(papf.start_date, 'YYYY/MM/DD')               start_date,
      to_char(papf.date_of_birth, 'YYYY/MM/DD')            date_of_birth,
    '' dateofdeath,
    '' countryofbirth,
    '' regionofbirth,
    '' townofbirth,
    '' applicantnumber,
    '' waivedataprotectflag,
    '' categorycode,
    'QIA_EBS' sourcesystemowner,
    papf.employee_number             AS sourcesystemid,
    CASE trunc(ppos.DATE_START)
        WHEN   trunc(papf.effective_start_date)  THEN
            'HIRE'
        ELSE
            'ASG_CHANGE'
    END AS action_code
FROM
    per_all_people_f           papf,
    per_person_type_usages_f   pptu,
    per_person_types ppt
    ,  per_all_assignments_f         paaf  
    , per_periods_of_service        ppos
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
    AND ppt.system_person_type = 'EMP'
    AND papf.person_type_id = ppt.person_type_id
         and  paaf.person_id = papf.person_id    
         and ppos.person_id = papf.person_id 
         --and papf.person_id  =15360
--         AND 1=2
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
--and nvl(ppos.actual_termination_date,'') >= trunc(sysdate)
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
-- or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate))
);

cursor C_PLD
is
SELECT  distinct  'MERGE' as MERGE , 'PersonLegislativeData' as PersonLegislativeData
, '' PersonLegislativeId,
  to_char(papf.effective_start_date, 'YYYY/MM/DD')     AS EffectiveStartDate,
    ''   AS EffectiveEndDate
,papf.employee_number PersonIdSourceSystemId,
papf.employee_number PersonNumber
,'QA' LegislationCode,
'' HighestEducationLevel
, papf.marital_status as MaritalStatus
,'' MaritalStatusDate
,papf.sex Sex 
,'QIA_EBS' SourceSystemOwner
,'PLD_'||papf.employee_number SourceSystemId
FROM PER_ALL_PEOPLE_F papf
,PER_PERSON_TYPE_USAGES_F pptu
,PER_PERSON_TYPES ppt
    ,  per_all_assignments_f         paaf
    , per_periods_of_service        ppos

WHERE pptu.person_id = papf.PERSON_ID
     and  paaf.person_id = papf.person_id    
AND pptu.person_type_id = ppt.person_type_id
AND ppt.system_person_type = 'EMP'
    AND papf.person_type_id = ppt.person_type_id
    and papf.person_id = ppos.person_id
--AND trunc(SYSDATE) between papf.effective_start_date and papf.effective_end_date
--AND trunc(SYSDATE) between pptu.effective_start_date and pptu.effective_end_date
--            AND trunc(SYSDATE) BETWEEN paaf.effective_start_date AND paaf.effective_end_date
--and papf.person_id  =15360
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
-- or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate))
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
);

cursor C_PN
is
SELECT distinct 'MERGE'as MERGE ,'PersonName' as PersonName,
'' PersonNameId,
  to_char(papf.effective_start_date, 'YYYY/MM/DD')     AS EffectiveStartDate ,
'' EffectiveEndDate
,papf.employee_number PersonId
  ,'QA' LegislationCode 
  ,'GLOBAL' NameType
  ,papf.first_name FirstName
  ,'' MiddleNames
  ,papf.last_name  LastName
,''Honors
,''PreNameAdjunct
,''MilitaryRank 
,''PreviousLastName 
,''Suffix
,papf.title
  ,'' CharSetContext
,'QIA_EBS' SourceSystemOwner,
'PN' ||'_'||papf.employee_number SourceSystemId
FROM PER_ALL_PEOPLE_F papf
,PER_PERSON_TYPE_USAGES_F pptu
,PER_PERSON_TYPES ppt
    ,  per_all_assignments_f         paaf
    , per_periods_of_service        ppos
WHERE pptu.person_id = papf.PERSON_ID
AND pptu.person_type_id = ppt.person_type_id
AND ppt.system_person_type = 'EMP'
    AND papf.person_type_id = ppt.person_type_id
     and  paaf.person_id = papf.person_id   
     and papf.person_id  =ppos.person_id
--     and papf.person_id = 33416
--AND trunc(SYSDATE) between papf.effective_start_date
--and papf.effective_end_date
--AND trunc(SYSDATE) between pptu.effective_start_date
--and pptu.effective_end_date 
-- AND trunc(SYSDATE) BETWEEN paaf.effective_start_date AND paaf.effective_end_date 
 and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
-- or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate))
)
--and papf.person_id  =15360
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
;


cursor c_PC
is 
SELECT distinct
    'MERGE' AS merge,
    'PersonCitizenship' personcitizenship,
    '' citizenshipid,
    papf.employee_number         personidsystemid,
    papf.employee_number   personnumber,
    to_char(papf.date_of_birth, 'YYYY/MM/DD')      datefrom,
    '' dateto,
    'QA'       leglisationcode,
    'Active' citizenshipstatus,
    'QIA_EBS' sourcesystemowner,
    'PC'
    || '_'
    || papf.employee_number sourcesystemid
FROM
    per_all_people_f           papf,
    per_person_type_usages_f   pptu,
    per_person_types           ppt
    ,  per_all_assignments_f         paaf    
    , per_periods_of_service        ppos
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
    AND ppt.system_person_type = 'EMP'
          and  paaf.person_id = papf.person_id
        AND papf.person_type_id = ppt.person_type_id
        and papf.person_id  =ppos.person_id
--        and papf.person_id = 33416
--and papf.person_id  =15360
--    AND trunc(SYSDATE) between papf.effective_start_date AND papf.effective_end_date
--    AND trunc(SYSDATE) BETWEEN pptu.effective_start_date AND pptu.effective_end_date
--            AND trunc(SYSDATE) BETWEEN paaf.effective_start_date AND paaf.effective_end_date 

and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
-- or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate))
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
);

cursor c_PNID
is
SELECT distinct
    'MERGE' merge,
    'PersonNationalIdentifier' personnationidentifier,
    '' nationalidentifierid,
    papf.employee_number         person_id,
    papf.employee_number   personnumber,
    'QA'       legislationcode,
    '' issuedate,
    '' expirationdate,
    '' placeofissue,
    'Social Insurance Number' nationalidentifiertype,
    replace(national_identifier, '-', NULL) nationalidentifiernumber,
    '' primaryflag,
    'QIA_EBS' sourcesystemowner,
    'PNID'
    || '_'
    || papf.employee_number sourcesystemid
FROM
    per_all_people_f           papf,
    per_person_type_usages_f   pptu,
    per_person_types           ppt
    ,  per_all_assignments_f         paaf
    , per_periods_of_service        ppos
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
        AND papf.person_type_id = ppt.person_type_id
      and  paaf.person_id = papf.person_id
    AND ppt.system_person_type = 'EMP'
        and papf.person_id  =ppos.person_id

--    and papf.person_id = 33416
--AND trunc(SYSDATE) BETWEEN papf.effective_start_date AND papf.effective_end_date
--    AND trunc(SYSDATE) BETWEEN pptu.effective_start_date AND pptu.effective_end_date 
--        AND trunc(SYSDATE) BETWEEN paaf.effective_start_date AND paaf.effective_end_date 
--and papf.person_id  =15360
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
-- or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate))
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
);



cursor c_email
is
select 


'MERGE' as metadata ,
'PersonEmail' as PersonEmail,
'' as EmailAddressId,
   to_char(papf.effective_start_date, 'YYYY/MM/DD')     AS DateFrom,
   ''  as DateTo ,
  papf.employee_number   person_id ,
  papf.employee_number   person_number ,
  'Work Email' EmailType,
    papf.EMAIL_ADDRESS as EmailAddress,
    'QIA_EBS' SourceSystemOwner,
    'MAIL_'||papf.employee_number    SourceSystemId

FROM
    per_all_people_f           papf,
    per_person_type_usages_f   pptu,
    per_person_types ppt
    ,  per_all_assignments_f         paaf  
    , per_periods_of_service        ppos
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
    AND ppt.system_person_type = 'EMP'
    AND papf.person_type_id = ppt.person_type_id
         and  paaf.person_id = papf.person_id    
         and ppos.person_id = papf.person_id 
         --and papf.person_id  =15360
--         AND 1=2
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
--and nvl(ppos.actual_termination_date,'') >= trunc(sysdate)
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
-- or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate))
);

cursor c_wrlp
is
SELECT distinct
    'MERGE' merge,
    'WorkRelationship' workrelationship,
    '' legalemployersenioritydate,
    '' actualterminationdate,
    '' legalentityid,
    '' comments,
to_char(papf.effective_start_date, 'YYYY/MM/DD')       enterprisesenioritydate,
    '' lastworkingdate,
   to_char(papf.effective_start_date, 'YYYY/MM/DD')       datestart,
    '' notifiedterminationdate,
    'N' onmilitaryserviceflag,
    '' periodofserviceid,
    papf.employee_number               personidsystemid,
    papf.current_employee_flag   primaryflag,
    '' projectedterminationdate,
    '' rehireauthorizerpersonid,
    '' rehireauthorizor,
    '' rehirereason,
    '' revokeuseraccess,
    '' workernumber,
    '' personnumber,
    'Qatar Investment Authority' legalemployername,
    '' rehirerecommendationflag,
    'E'       workertype,'' guid,
    'WRLP'
    || '_'
    || papf.employee_number sourcesystemid,
    'QIA_EBS' sourcesystemowner
FROM
    per_all_people_f           papf,
    per_person_type_usages_f   pptu,
    per_person_types           ppt,
     per_all_assignments_f         paaf
     , per_periods_of_service        ppos
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
    AND papf.person_type_id = ppt.person_type_id
    and paaf.person_id = papf.person_id
    and papf.person_id  =ppos.person_id
    AND ppt.system_person_type = 'EMP'
--    and papf.person_id = 33416
--    AND trunc(SYSDATE) BETWEEN papf.effective_start_date AND papf.effective_end_date
--    AND trunc(SYSDATE) BETWEEN pptu.effective_start_date AND pptu.effective_end_date
--        AND trunc(SYSDATE) BETWEEN paaf.effective_start_date AND paaf.effective_end_date
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
-- or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate))
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
);




-- cursor c_pp_asn
-- is
--  select distinct papf.person_id person_id from 
--     per_all_people_f              papf,
--    per_all_assignments_f         paaf
-- where
--     papf.person_id = paaf.person_id
--    --and papf.person_id  =15360
-- and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
-- or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate)))
-- order by papf.person_id;

cursor c_wt
is
SELECT distinct
    'MERGE' AS merge,
    'WorkTerms' AS workterms,
     CASE trunc(ppos.DATE_START)
  WHEN   trunc(paaf.effective_start_date)  THEN
    'HIRE'
    else 
    'ASG_CHANGE'
    end AS actioncode,
    papf.employee_number         AS assignmentid,
    'WRLP'
    || '_'
    || papf.employee_number periodofserviceid,
    papf.employee_number              personid,
    '' AS assignmentname,
    '' AS assignmentnumber,
    'ACTIVE_PROCESS'     assignmentstatustypecode,
    '' AS assignmentstatustypeid,
    'ET'        assignmenttype,
    '' bargainingunitcode,
    '' billingtitle,
    '' businessunitid,
    'QIA' businessunitshortcode,
    '' collectiveagreementid,
    '' contractid,
    '' dateprobationend,
to_char(paaf.effective_start_date, 'YYYY/MM/DD')       effectivestartdate,
to_char(paaf.effective_end_date, 'YYYY/MM/DD')    effectiveenddate,
    '1' effectivesequence,
    'Y' effectivelatestchange,
    'Qatar Investment Authority' legalemployername
,    '' persontypeid,
    ppt.user_person_type        persontypecode,
    '' positionid,
   ''                  positioncode,
    'N' positionoverrideflag,
    'Y' primaryworktermsflag,
    'EMP' systempersontype,
    ''FreezeStartDate,
    '' freezeuntildate,
    '' guid,
    'QIA_EBS' sourcesystemowner,
    'WT'
    || '_'
    || papf.employee_number sourcesystemid,
    'R' permanenttemporary
FROM
    per_all_people_f              papf,
    per_person_type_usages_f      pptu,
    per_person_types              ppt,
    per_all_assignments_f         paaf,
    per_periods_of_service        ppos,
    per_assignment_status_types   past,
    hr_all_positions_f            hapf
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
    AND papf.person_type_id = ppt.person_type_id
    AND ppt.system_person_type = 'EMP'
    AND papf.person_id = paaf.person_id
    AND paaf.person_id = pptu.person_id
    AND papf.person_id = ppos.person_id
    AND paaf.person_id = ppos.person_id
    AND paaf.assignment_status_type_id = past.assignment_status_type_id
    AND paaf.position_id = hapf.position_id (+)
    --and papf.person_id  =15360
--    and papf.person_id = 33416
    AND trunc(SYSDATE) BETWEEN papf.effective_start_date AND papf.effective_end_date
--    AND trunc(SYSDATE) BETWEEN pptu.effective_start_date AND pptu.effective_end_date
--    AND trunc(SYSDATE) BETWEEN paaf.effective_start_date AND paaf.effective_end_date
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate))) order by     papf.employee_number, to_char(paaf.effective_start_date, 'YYYY/MM/DD') ;


 

cursor c_asn is
SELECT distinct
    'MERGE' AS merge,
    'Assignment' assignment,
    CASE trunc(ppos.DATE_START)
  WHEN   trunc(paaf.effective_start_date)  THEN
            'HIRE'
        ELSE
            'ASG_CHANGE'
    END AS actioncode,
    '' reasoncode,
    papf.employee_number              personid,
    '' assignmentid,
    papf.employee_number              sourcesystemid,
    'WRLP'
    || '_'
    || papf.employee_number periodofserviceid,
  to_char(paaf.effective_start_date, 'YYYY/MM/DD')    effectivestartdate,
    to_char(paaf.effective_end_date, 'YYYY/MM/DD') effectiveenddate,
    '1' effectivesequence,
    'Y' effectivelatestchange,
    'E' assignmenttype,
    '' assignmentname,
    '' assignmentnumber,
    'ACTIVE_PROCESS'     assignmentstatustypecode,
    '' assignmentstatustypeid,
    '' bargainingunitcode,
    'QIA' businessunitshortcode,
    '' collectiveagreementid,
        '' collectiveagreementidcode,
    '' dateprobationend,
    'WC' workercategory,
    paaf.assignment_category    assignmentcategory,
    '' establishmentid,
    '' reportingestablishment,
    '' gradeid,
    pg.grade_id                gradecode,
    '' hourlysalariedcode,
    '' jobid,
    hapf.position_id jobcode,
    '' labourunionmemberflag,
    'QIA_HO'          locationcode,
    '' locationid,
    'N' managerflag,
    '' normalhours,
    '' frequency,
    '' noticeperiod,
    '' noticeperioduom,
    paaf.organization_id        organization_id,
    '' personnumber,
    '' datestart,
    '' workertype,
    'Qatar Investment Authority' legalemployername,
    ppt.user_person_type        persontypecode,
    '' persontypeid,
    '' positioncode,
    '' positionid,
    'N' positionoverrideflag,
    'Y' primaryassignmentflag,
    'Y' primaryflag,
    '6' probationperiod,
    'Months' probationunit,
    '' projecttitle,
    '' projectedenddate,
    '' projectedstartdate,
    '' proposeduserpersontype,
    '' proposedworkertype,
    '' retirementage,
    '' retirementdate,
    '' specialceilingstep,
    '' specialceilingstepid,
    'EMP' systempersontype,
    '' taxaddressid,
    '' endtime,
    '' starttime,
    '' vendorsiteid,
    '' workathomeflag,
    '' worktermsnumber,
    'WT'
    || '_'
    || papf.employee_number worktermsassignmentid,
    '' vendorid,
    '1951/01/01' freezestartdate,
    '4712/12/31' freezeuntildate,
    'QIA_EBS' sourcesystemowner
FROM
    per_all_people_f              papf,
    per_person_type_usages_f      pptu,
    per_person_types              ppt,
    per_all_assignments_f         paaf,
    per_periods_of_service        ppos,
    per_assignment_status_types   past,
    hr_all_positions_f            hapf,
    per_grades                    pg,
    per_jobs                      pj,
    hr_locations_all              hla
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
    AND papf.person_type_id = ppt.person_type_id
    AND ppt.system_person_type = 'EMP'
    AND papf.person_id = paaf.person_id
    AND paaf.person_id = pptu.person_id
    AND papf.person_id = ppos.person_id
    AND paaf.person_id = ppos.person_id
    AND paaf.assignment_status_type_id = past.assignment_status_type_id
    AND paaf.position_id = hapf.position_id (+)
    AND paaf.grade_id = pg.grade_id (+)
    AND paaf.job_id = pj.job_id (+)
    AND paaf.location_id = hla.location_id (+)
--    and papf.person_id  = 15360
--    and papf.person_id = 33416
    AND  trunc(SYSDATE) BETWEEN papf.effective_start_date AND papf.effective_end_date
--    AND  trunc(SYSDATE) BETWEEN pptu.effective_start_date AND pptu.effective_end_date
--    AND  trunc(SYSDATE) BETWEEN paaf.effective_start_date AND paaf.effective_end_date
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate)))
order by  papf.employee_number, to_char(paaf.effective_start_date, 'YYYY/MM/DD')    ,
    to_char(paaf.effective_end_date, 'YYYY/MM/DD') ;


cursor c_contract
is 
SELECT distinct  'MERGE' as MERGE,'Contract' as Contract, 
papf.employee_number as AssignmentId,
'' ContractId , '' ContractType ,   '' Description      ,
'' Duration           , '' DurationUnits
,  to_char(papf.effective_start_date, 'YYYY/MM/DD')       as EffectiveStartDate
,''            EffectiveEndDate
,papf.employee_number pers_id
,'CT_'||papf.employee_number  as SourceSystemId,
'QIA_EBS' SourceSystemOwner
FROM PER_ALL_PEOPLE_F papf
,PER_PERSON_TYPE_USAGES_F pptu
,PER_PERSON_TYPES ppt
,per_all_assignments_f paaf
,per_periods_of_service        ppos
WHERE pptu.person_id = papf.PERSON_ID
AND pptu.person_type_id = ppt.person_type_id
and papf.person_id=paaf.person_id
AND ppt.system_person_type = 'EMP'
--and papf.person_id  =15360
--and papf.person_id = 33416
AND trunc(SYSDATE) between PAPF.effective_start_date
and PAPF.effective_end_date
--AND trunc(SYSDATE) between pptu.effective_start_date
--and pptu.effective_end_date
AND paaf.person_id = ppos.person_id
  AND  trunc(ppos.DATE_START)=   trunc(paaf.effective_start_date)
  and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate)));

cursor c_ass_sup
is 
SELECT DISTINCT 'MERGE'  AS MERGE,
  'AssignmentSupervisor' AS AssignmentSupervisor,
  papf.employee_number   AS Assignmentid,
  TO_CHAR(paaf.effective_end_date, 'YYYY/MM/DD') EffectiveendtDate,
  TO_CHAR(paaf.effective_start_date, 'YYYY/MM/DD') EffectiveStartDate,
  papf1.employee_number ManagerassignmentNumber,
  papf1.employee_number Managerid,
  'LINE_MANAGER' ManagerType,
  papf.employee_number personNumber,
  --                                papf1.employee_number ManagerPersonNumber,
  'Y' PrimaryFlag,
  papf1.employee_number ManagerpersonNumber,
  'QIA_EBS' SourceSystemOwner ,
  'AS_'
  ||papf.employee_number SourceSystemId,
  CASE TRUNC(ppos.DATE_START)
    WHEN TRUNC(paaf.effective_start_date)
    THEN 'HIRE'
    ELSE 'ASG_CHANGE'
  END AS actioncode
FROM PER_ALL_PEOPLE_F papf ,
  per_all_people_f papf1 ,
  PER_PERSON_TYPE_USAGES_F pptu ,
  PER_PERSON_TYPES ppt ,
  per_all_assignments_f paaf ,
  per_periods_of_service ppos
WHERE pptu.person_id       = papf.PERSON_ID
AND paaf.person_id         =papf.person_id
AND papf1.person_id        = paaf.supervisor_id
AND pptu.person_type_id    = ppt.person_type_id
AND ppt.system_person_type = 'EMP'
AND papf.person_id         =ppos.person_id
AND TRUNC(SYSDATE) BETWEEN papf.effective_start_date AND papf.effective_end_date
AND TRUNC(SYSDATE) BETWEEN papf1.effective_start_date AND papf1.effective_end_date
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
AND ( (TRUNC(papf.LAST_UPDATE_DATE) = TRUNC(sysdate))
OR( TRUNC(paaf.LAST_UPDATE_DATE)    = TRUNC(sysdate)));



    l_file_name varchar2(200);
    lv_check_file_exist      boolean;
    l_global_file            varchar2 (100);
          lv_a                     number;
      lv_b                     number;
        l_dir_name               varchar2 (100);
        l_file_handle            UTL_FILE.file_type;
        l_cnt number;
        

begin
      DBMS_OUTPUT.enable (200000000000);

l_file_name := p_file_name ||'.dat';
l_dir_name := '/usr/tmp';
  UTL_FILE.fgetattr (l_dir_name,
                         l_file_name,
                         lv_check_file_exist,
                         lv_a,
                         lv_b);

     fnd_file.put_line (fnd_file.LOG,       'Note: File will be generated in this path: ' || l_dir_name     );                         
--l_global_file := l_file_name;
      l_file_handle :=
         UTL_FILE.fopen (l_dir_name,
                         l_file_name,
                         'W',
                         32767);


             ----Worker

--                     UTL_FILE.put_line( l_file_handle,  
--                     'METADATA|Worker|PersonId|EffectiveStartDate|EffectiveEndDate|PersonNumber|BloodType|CorrespondenceLanguage|StartDate|DateOfBirth|DateOfDeath|CountryOfBirth|RegionOfBirth|TownOfBirth|ApplicantNumber|WaiveDataProtectFlag|CategoryCode|SourceSystemOwner|SourceSystemId|ActionCode');
       ----Worker   data

--open c_Worker;
--l_cnt := c_Worker%rowcount;
--if l_cnt = 0 then
--UTL_FILE.put_line( l_file_handle,  
--                     'METADATA|Worker|PersonId|EffectiveStartDate|EffectiveEndDate|PersonNumber|BloodType|CorrespondenceLanguage|StartDate|DateOfBirth|DateOfDeath|CountryOfBirth|RegionOfBirth|TownOfBirth|ApplicantNumber|WaiveDataProtectFlag|CategoryCode|SourceSystemOwner|SourceSystemId|ActionCode');
--       --Worker   data
--end if;
--close c_Worker;



     l_cnt:= 0;                     
    FOR i IN c_Worker
      LOOP

          if l_cnt = 0 then  
  UTL_FILE.put_line( l_file_handle,  
                     'METADATA|Worker|PersonId|EffectiveStartDate|EffectiveEndDate|PersonNumber|BloodType|CorrespondenceLanguage|StartDate|DateOfBirth|DateOfDeath|CountryOfBirth|RegionOfBirth|TownOfBirth|ApplicantNumber|WaiveDataProtectFlag|CategoryCode|SourceSystemOwner|SourceSystemId|ActionCode');      
      end if;

         UTL_FILE.put_line (
            l_file_handle,
    i.merge  || '|' 
   || i.worker    || '|' 
   || i.person_id    || '|' 
   || i.effective_start_date    || '|' 
   || i.effective_end_date    || '|' 
   || i.employee_number    || '|' 
   || i.bloodtype          || '|'
   || i.correspondencelanguage    || '|' 
   || i.start_date    || '|' 
   || i.date_of_birth    || '|' 
   || i.dateofdeath    || '|' 
   || i.countryofbirth    || '|' 
   || i.regionofbirth    || '|' 
   || i.townofbirth    || '|' 
   || i.applicantnumber    || '|' 
   || i.waivedataprotectflag    || '|' 
   || i.categorycode    || '|' 
   || i.sourcesystemowner    || '|' 
   || i.sourcesystemid    || '|' 
   || i.action_code       
         );

   l_cnt := l_cnt +1;

        end loop;
         l_cnt:= 0;    
--      ---- PersonLegislativeData  header
--        UTL_FILE.put_line (
--            l_file_handle, 'METADATA|PersonLegislativeData|PersonLegislativeId|EffectiveStartDate|EffectiveEndDate|PersonId(SourceSystemId)|PersonNumber|LegislationCode|HighestEducationLevel|MaritalStatus|MaritalStatusDate|Sex|SourceSystemOwner|SourceSystemId');
       ----PersonLegislativeData  data
--       
--       open C_PLD;
--l_cnt := C_PLD%rowcount;
--if l_cnt = 0 then
--        UTL_FILE.put_line (
--            l_file_handle, 'METADATA|PersonLegislativeData|PersonLegislativeId|EffectiveStartDate|EffectiveEndDate|PersonId(SourceSystemId)|PersonNumber|LegislationCode|HighestEducationLevel|MaritalStatus|MaritalStatusDate|Sex|SourceSystemOwner|SourceSystemId');
--    --Worker   data
--end if;
--close C_PLD;
----       
        l_cnt := 0 ;
       
        for j in C_PLD
        loop


    if l_cnt = 0 then  
    UTL_FILE.put_line (
            l_file_handle, 'METADATA|PersonLegislativeData|PersonLegislativeId|EffectiveStartDate|EffectiveEndDate|PersonId(SourceSystemId)|PersonNumber|LegislationCode|HighestEducationLevel|MaritalStatus|MaritalStatusDate|Sex|SourceSystemOwner|SourceSystemId');
      end if;      


           UTL_FILE.put_line (
            l_file_handle,
    j.MERGE  || '|' 
   || j.PersonLegislativeData    || '|' 
   || j.PersonLegislativeId    || '|' 
   || j.EffectiveStartDate    || '|' 
   || j.EffectiveEndDate    || '|' 
   || j.PersonIdSourceSystemId    || '|' 
   || j.PersonNumber    || '|' 
   || j.LegislationCode  || '|' 
   || j.HighestEducationLevel  || '|' 
   || j.MaritalStatus  || '|' 
  || j.MaritalStatusdate  || '|'
   || j.Sex  || '|'
   || j.SourceSystemOwner  || '|' 
   || j.SourceSystemId
         );
          l_cnt := l_cnt +1;
        end loop;


--      ---- PersonName  header
          
       ----PersonName  data
       
--open C_PN;
--l_cnt := C_PN%rowcount;
--if l_cnt = 0 then
--        UTL_FILE.put_line (
--              l_file_handle, 'METADATA|PersonName|PersonNameId|EffectiveStartDate|EffectiveEndDate|PersonId(SourceSystemId)|LegislationCode|NameType|FirstName|MiddleNames|LastName|Honors|PreNameAdjunct|MilitaryRank|PreviousLastName|Suffix|Title|CharSetContext|SourceSystemOwner|SourceSystemId');
--   --Worker   data
--end if;
--close C_PN;      
       
         l_cnt := 0;
        for j in C_PN
        loop
      if l_cnt = 0 then  
     UTL_FILE.put_line (
            l_file_handle, 'METADATA|PersonName|PersonNameId|EffectiveStartDate|EffectiveEndDate|PersonId(SourceSystemId)|LegislationCode|NameType|FirstName|MiddleNames|LastName|Honors|PreNameAdjunct|MilitaryRank|PreviousLastName|Suffix|Title|CharSetContext|SourceSystemOwner|SourceSystemId');
      end if;  
           UTL_FILE.put_line (
            l_file_handle,
    j.MERGE  || '|' 
   || j.PersonName    || '|' 
   || j.PersonNameId    || '|' 
   || j.EffectiveStartDate    || '|' 
   || j.EffectiveEndDate    || '|' 
   || j.PersonId    || '|' 
   || j.LegislationCode    || '|' 
   || j.NameType  || '|' 
   || j.FirstName  || '|' 
   || j.MiddleNames  || '|'
   || j.LastName  || '|' 
   || j.Honors || '|'
   || j.PreNameAdjunct || '|'
   || j.MilitaryRank || '|'
   || j.PreviousLastName || '|'
   || j.Suffix || '|'
   || j.title || '|'
   || j.CharSetContext || '|'
   || j.SourceSystemOwner || '|'
   || j.SourceSystemId 

         );
          l_cnt := l_cnt +1;
        end loop;




--open c_PC;
--l_cnt := c_PC%rowcount;
--if l_cnt = 0 then
--        UTL_FILE.put_line (
--               l_file_handle, 'METADATA|PersonCitizenship|CitizenshipId|PersonId(SourceSystemId)|PersonNumber|DateFrom|DateTo|LegislationCode|CitizenshipStatus|SourceSystemOwner|SourceSystemId');
--    --Worker   data
--end if;
--close c_PC; 

--      ---- PersonCitizenship  header
   l_cnt := 0 ;
       ----PersonCitizenship  data
        for j in c_PC
        loop

      if l_cnt = 0 then  
     UTL_FILE.put_line (
              l_file_handle, 'METADATA|PersonCitizenship|CitizenshipId|PersonId(SourceSystemId)|PersonNumber|DateFrom|DateTo|LegislationCode|CitizenshipStatus|SourceSystemOwner|SourceSystemId');
      end if;        



           UTL_FILE.put_line (
            l_file_handle,
    j.MERGE  || '|' 
   || j.personcitizenship    || '|' 
   || j.citizenshipid    || '|' 
   || j.personidsystemid    || '|' 
   || j.personnumber    || '|' 
   || j.datefrom    || '|' 
   || j.dateto  || '|' 
   || j.leglisationcode  || '|' 
   || j.citizenshipstatus  || '|'
   || j.sourcesystemowner  || '|' 
   || j.sourcesystemid

         );
          l_cnt := l_cnt +1;
        end loop; 



--      ---- PersonNationIdentifier  header


--open c_PNID;
--l_cnt := c_PNID%rowcount;
--if l_cnt = 0 then
--        UTL_FILE.put_line (
--                   l_file_handle, 'METADATA|PersonNationalIdentifier|NationalIdentifierId|PersonId(SourceSystemId)|PersonNumber|LegislationCode|IssueDate|ExpirationDate|PlaceOfIssue|NationalIdentifierType|NationalIdentifierNumber|PrimaryFlag|SourceSystemOwner|SourceSystemId');
--end if;
--close c_PNID; 


      l_cnt := 0 ;
       ----PersonNationIdentifier  data
        for j in c_PNID
        loop
--        
     if l_cnt = 0 then  
    UTL_FILE.put_line (
            l_file_handle, 'METADATA|PersonNationalIdentifier|NationalIdentifierId|PersonId(SourceSystemId)|PersonNumber|LegislationCode|IssueDate|ExpirationDate|PlaceOfIssue|NationalIdentifierType|NationalIdentifierNumber|PrimaryFlag|SourceSystemOwner|SourceSystemId');

      end if;            
           UTL_FILE.put_line (
            l_file_handle,
    j.MERGE  || '|' 
   || j.PersonNationIdentifier    || '|' 
   || j.nationalidentifierid    || '|' 
   || j.person_id    || '|' 
   || j.personnumber    || '|' 
   || j.legislationcode    || '|' 
   || j.issuedate  || '|' 
   || j.expirationdate  || '|' 
   || j.placeofissue  || '|'
   || j.nationalidentifiertype  || '|' 
   || j.nationalidentifiernumber || '|' 
      || j.primaryflag  || '|' 
   || j.sourcesystemowner  || '|' 
   || j.sourcesystemid   


         );
          l_cnt := l_cnt +1;
        end loop;         
        
        
-- c_email        
        
        
      l_cnt := 0 ;
       ----WorkRelationship  data
        for j in c_email
        loop
        if   l_cnt = 0  then
   UTL_FILE.put_line (
l_file_handle, 'METADATA|PersonEmail|EmailAddressId|DateFrom|DateTo|PersonId|PersonNumber|EmailType|EmailAddress|SourceSystemOwner|SourceSystemId');
     end if;     
     
       UTL_FILE.put_line (
            l_file_handle,
j.METADATA  || '|'
|| j.PERSONEMAIL  || '|' 
|| j.EMAILADDRESSID|| '|' 
|| j.DATEFROM || '|' 
|| j.DATETO || '|' 
|| j.PERSON_ID  || '|' 
|| j.PERSON_NUMBER  || '|' 
|| j.EMAILTYPE || '|' 
|| j.EMAILADDRESS || '|' 
|| j.SOURCESYSTEMOWNER || '|' 
|| j.SOURCESYSTEMID 
);

        
            l_cnt := l_cnt +1;
        end loop;    

        
        
 ---- WorkRelationship  header



--open c_wrlp;
--l_cnt := c_wrlp%rowcount;
--if l_cnt = 0 then
--        UTL_FILE.put_line (
--                   l_file_handle, 'METADATA|WorkRelationship|LegalEmployerSeniorityDate|ActualTerminationDate|LegalEntityId|Comments|EnterpriseSeniorityDate|LastWorkingDate|DateStart|NotifiedTerminationDate|OnMilitaryServiceFlag|PeriodOfServiceId|PersonId(SourceSystemId)|PrimaryFlag|ProjectedTerminationDate|RehireAuthorizerPersonId|RehireAuthorizor|RehireReason|RevokeUserAccess|WorkerNumber|PersonNumber|LegalEmployerName|RehireRecommendationFlag|WorkerType|GUID|SourceSystemId|SourceSystemOwner');
--end if;
--close c_wrlp; 
--

      l_cnt := 0 ;
       ----WorkRelationship  data
        for j in c_wrlp
        loop
        if   l_cnt = 0  then
   UTL_FILE.put_line (
            l_file_handle, 'METADATA|WorkRelationship|LegalEmployerSeniorityDate|ActualTerminationDate|LegalEntityId|Comments|EnterpriseSeniorityDate|LastWorkingDate|DateStart|NotifiedTerminationDate|OnMilitaryServiceFlag|PeriodOfServiceId|PersonId(SourceSystemId)|PrimaryFlag|ProjectedTerminationDate|RehireAuthorizerPersonId|RehireAuthorizor|RehireReason|RevokeUserAccess|WorkerNumber|PersonNumber|LegalEmployerName|RehireRecommendationFlag|WorkerType|GUID|SourceSystemId|SourceSystemOwner');
     end if;        

           UTL_FILE.put_line (
            l_file_handle,
    j.MERGE  || '|' 
   || j.workrelationship    || '|' 
   || j.legalemployersenioritydate    || '|' 
   || j.actualterminationdate    || '|' 
   || j.legalentityid    || '|' 
   || j.comments    || '|' 
   || j.enterprisesenioritydate  || '|' 
   || j.lastworkingdate  || '|' 
   || j.datestart  || '|'
   || j.notifiedterminationdate  || '|' 
   || j.onmilitaryserviceflag || '|' 
      || j.periodofserviceid  || '|' 
   || j.personidsystemid  || '|' 
   || j.primaryflag    || '|' 
 || j.projectedterminationdate    || '|' 
  || j.rehireauthorizerpersonid    || '|' 
   || j.rehireauthorizor    || '|' 
    || j.rehirereason    || '|' 
     || j.revokeuseraccess    || '|' 
      || j.workernumber    || '|' 
       || j.personnumber    || '|' 
      || j.legalemployername    || '|' 
            || j.rehirerecommendationflag    || '|' 
                  || j.workertype    || '|' 
           || j.guid    || '|' 
         || j.sourcesystemid    || '|' 
                 || j.sourcesystemowner

         );
            l_cnt := l_cnt +1;
        end loop;    



--      ---- WorkTerms  header

       ----WorkTerms  data



--open c_wt;
--l_cnt := c_wt%rowcount;
--if l_cnt = 0 then
--        UTL_FILE.put_line (
--                      l_file_handle, 'METADATA|WorkTerms|ActionCode|AssignmentId|PeriodOfServiceId(SourceSystemId)|PersonId(SourceSystemId)|AssignmentName|AssignmentNumber|AssignmentStatusTypeCode|AssignmentStatusTypeId|AssignmentType|BargainingUnitCode|BillingTitle|BusinessUnitId|BusinessUnitShortCode|CollectiveAgreementId|ContractId|DateProbationEnd|EffectiveStartDate|EffectiveEndDate|EffectiveSequence|EffectiveLatestChange|LegalEmployerName|PersonTypeId|PersonTypeCode|PositionId|PositionCode|PositionOverrideFlag|PrimaryWorkTermsFlag|SystemPersonType|FreezeStartDate|FreezeUntilDate|GUID|SourceSystemOwner|SourceSystemId|PermanentTemporary');
--end if;
--close c_wt; 
l_cnt := 0 ;
        for j in c_wt
        loop
      

if l_cnt = 0 then
        UTL_FILE.put_line (
                      l_file_handle, 'METADATA|WorkTerms|ActionCode|AssignmentId|PeriodOfServiceId(SourceSystemId)|PersonId(SourceSystemId)|AssignmentName|AssignmentNumber|AssignmentStatusTypeCode|AssignmentStatusTypeId|AssignmentType|BargainingUnitCode|BillingTitle|BusinessUnitId|BusinessUnitShortCode|CollectiveAgreementId|ContractId|DateProbationEnd|EffectiveStartDate|EffectiveEndDate|EffectiveSequence|EffectiveLatestChange|LegalEmployerName|PersonTypeId|PersonTypeCode|PositionId|PositionCode|PositionOverrideFlag|PrimaryWorkTermsFlag|SystemPersonType|FreezeStartDate|FreezeUntilDate|GUID|SourceSystemOwner|SourceSystemId|PermanentTemporary');
end if;


           UTL_FILE.put_line (
            l_file_handle,
    j.MERGE  || '|' 
                                                                || j.workterms    || '|' 
                                                                || j.actioncode    || '|' 
                                                                || j.assignmentid    || '|' 
                                                                || j.periodofserviceid    || '|' 
                                                                || j.personid    || '|' 
                                                                || j.assignmentname  || '|' 
                                                                || j.assignmentnumber  || '|' 
                                                                || j.assignmentstatustypecode  || '|'
                                                                || j.assignmentstatustypeid  || '|' 
                                                                || j.assignmenttype || '|' 
                                                                || j.bargainingunitcode  || '|' 
                                                                || j.billingtitle  || '|' 
                                                                || j.businessunitid    || '|' 
                                                                || j.businessunitshortcode    || '|' 
                                                                || j.collectiveagreementid    || '|' 
                                                                || j.contractid    || '|' 
                                                                || j.dateprobationend    || '|' 
                                                                || j.effectivestartdate    || '|' 
                                                                || j.effectiveenddate    || '|' 
                                                || j.effectivesequence    || '|' 
                                                                || j.effectivelatestchange    || '|' 
                                                                || j.legalemployername    || '|' 
                                                                || j.persontypeid    || '|' 
                                                                || j.persontypecode    || '|' 
                                                                || j.positionid    || '|' 
                                                                || j.positioncode  || '|' 
                                                                || j.positionoverrideflag  || '|' 
                                                                || j.primaryworktermsflag  || '|' 
                                                                || j.systempersontype  || '|' 
                || j.FreezeStartDate || '|' 

                                                                || j.freezeuntildate  || '|' 
                                                                || j.guid  || '|' 
                                                                || j.sourcesystemowner  || '|' 
                                                                || j.sourcesystemid  || '|' 
                                                                || j.permanenttemporary

         );
                     l_cnt := l_cnt +1;

        end loop;  
--open c_asn;
--l_cnt := c_asn%rowcount;
--if l_cnt = 0 then
--        UTL_FILE.put_line (
--                         l_file_handle, 'METADATA|Assignment|ActionCode|ReasonCode|PersonId(SourceSystemId)|AssignmentId|SourceSystemId|PeriodOfServiceId(SourceSystemId)|EffectiveStartDate|EffectiveEndDate|EffectiveSequence|EffectiveLatestChange|AssignmentType|AssignmentName|AssignmentNumber|AssignmentStatusTypeCode|AssignmentStatusTypeId|BargainingUnitCode|BusinessUnitShortCode|CollectiveAgreementId|CollectiveAgreementIdCode|DateProbationEnd|WorkerCategory|AssignmentCategory|EstablishmentId|ReportingEstablishment|GradeId(SourceSystemId)|GradeCode|HourlySalariedCode|JobId(SourceSystemId)|JobCode|LabourUnionMemberFlag|LocationCode|LocationId|ManagerFlag|NormalHours|Frequency|NoticePeriod|NoticePeriodUOM|OrganizationId(SourceSystemId)|PersonNumber|DateStart|WorkerType|LegalEmployerName|PersonTypeCode|PersonTypeId|PositionCode|PositionId|PositionOverrideFlag|PrimaryAssignmentFlag|PrimaryFlag|ProbationPeriod|ProbationUnit|ProjectTitle|ProjectedEndDate|ProjectedStartDate|ProposedUserPersonType|ProposedWorkerType|RetirementAge|RetirementDate|SpecialCeilingStep|SpecialCeilingStepId|SystemPersonType|TaxAddressId|EndTime|StartTime|VendorSiteId|WorkAtHomeFlag|WorkTermsNumber|WorkTermsAssignmentId(SourceSystemId)|VendorId|FreezeStartDate|FreezeUntilDate|SourceSystemOwner');
--end if;
--close c_asn; 


   ---- c_asn  header

     
  
     l_cnt := 0 ;
        for j in c_asn
        loop
        if l_cnt = 0 then
        UTL_FILE.put_line (
                         l_file_handle, 'METADATA|Assignment|ActionCode|ReasonCode|PersonId(SourceSystemId)|AssignmentId|SourceSystemId|PeriodOfServiceId(SourceSystemId)|EffectiveStartDate|EffectiveEndDate|EffectiveSequence|EffectiveLatestChange|AssignmentType|AssignmentName|AssignmentNumber|AssignmentStatusTypeCode|AssignmentStatusTypeId|BargainingUnitCode|BusinessUnitShortCode|CollectiveAgreementId|CollectiveAgreementIdCode|DateProbationEnd|WorkerCategory|AssignmentCategory|EstablishmentId|ReportingEstablishment|GradeId(SourceSystemId)|GradeCode|HourlySalariedCode|JobId(SourceSystemId)|JobCode|LabourUnionMemberFlag|LocationCode|LocationId|ManagerFlag|NormalHours|Frequency|NoticePeriod|NoticePeriodUOM|OrganizationId(SourceSystemId)|PersonNumber|DateStart|WorkerType|LegalEmployerName|PersonTypeCode|PersonTypeId|PositionCode|PositionId|PositionOverrideFlag|PrimaryAssignmentFlag|PrimaryFlag|ProbationPeriod|ProbationUnit|ProjectTitle|ProjectedEndDate|ProjectedStartDate|ProposedUserPersonType|ProposedWorkerType|RetirementAge|RetirementDate|SpecialCeilingStep|SpecialCeilingStepId|SystemPersonType|TaxAddressId|EndTime|StartTime|VendorSiteId|WorkAtHomeFlag|WorkTermsNumber|WorkTermsAssignmentId(SourceSystemId)|VendorId|FreezeStartDate|FreezeUntilDate|SourceSystemOwner');
end if;

           UTL_FILE.put_line (
            l_file_handle,
    j.MERGE  || '|' 
                                                                || j.assignment    || '|' 
                                                                || j.actioncode    || '|' 
                                                                || j.reasoncode    || '|' 
                                                                || j.personid    || '|' 
                                                                || j.AssignmentId    || '|' 
                                                                || j.SourceSystemId  || '|' 
                                                                || j.periodofserviceid  || '|' 
                                                                || j.EffectiveStartDate  || '|'
                                                                || j.EffectiveEndDate  || '|' 
                                                || j.EffectiveSequence || '|' 
                                                                || j.effectivelatestchange  || '|' 
                                                                || j.assignmenttype  || '|' 
                                                                || j.assignmentname    || '|' 
                                                                || j.assignmentnumber    || '|' 
                                                                || j.assignmentstatustypecode    || '|' 
                                                                || j.assignmentstatustypeid    || '|' 
                                                                || j.bargainingunitcode    || '|' 
                                                                || j.businessunitshortcode    || '|' 
                                                                || j.collectiveagreementid    || '|' 
                                                                || j.collectiveagreementidcode    || '|' 
                                                                || j.dateprobationend    || '|' 
                                                                || j.workercategory    || '|' 
                                                                || j.assignmentcategory    || '|' 
                                                                || j.establishmentid    || '|' 
                                                                || j.reportingestablishment    || '|' 
                                                                || j.gradeid  || '|' 
                                                                || j.gradecode  || '|' 
                                                                || j.hourlysalariedcode  || '|' 
                                                                || j.jobid  || '|' 
                                                                || j.jobcode  || '|' 
                                                                || j.labourunionmemberflag  || '|' 
                                                                || j.locationcode  || '|' 
                                                                || j.locationid  || '|' 
                                                                || j.managerflag || '|' 
                || j.normalhours || '|' 
                || j.frequency || '|' 
                || j.noticeperiod || '|' 
                || j.noticeperioduom || '|' 
                || j.organization_id || '|' 
                || j.personnumber || '|' 
                || j.datestart || '|' 
                || j.workertype || '|' 
                || j.legalemployername || '|' 
                || j.persontypecode || '|' 
                || j.persontypeid || '|' 
                || j.positioncode || '|' 
                || j.positionid || '|' 
                || j.positionoverrideflag || '|' 
                || j.primaryassignmentflag || '|' 
                || j.primaryflag || '|' 
                || j.probationperiod || '|' 
                || j.probationunit || '|' 
                || j.projecttitle || '|' 
                || j.projectedenddate || '|' 
                || j.projectedstartdate || '|' 
                || j.proposeduserpersontype || '|' 
                || j.proposedworkertype || '|' 
                || j.retirementage || '|' 
                || j.retirementdate || '|' 
                || j.specialceilingstep || '|' 
                || j.specialceilingstepid || '|' 
                || j.systempersontype || '|' 
                || j.taxaddressid    || '|' 
                || j.endtime || '|' 
                || j.starttime || '|' 
                || j.vendorsiteid || '|' 
                || j.workathomeflag || '|' 
                || j.worktermsnumber || '|' 
                || j.worktermsassignmentid || '|' 
                || j.vendorid || '|' 
                || j.freezestartdate     || '|' 
                || j.freezeuntildate || '|' 
                || j.sourcesystemowner   
         );
                     l_cnt := l_cnt +1;

end loop;
  ---- c_contract  header
       ----c_contract  data
       

--open c_contract;
--l_cnt := c_contract%rowcount;
--if l_cnt = 0 then
--      UTL_FILE.put_line (
--            l_file_handle, 'METADATA|Contract|AssignmentId|ContractId|ContractType|Description|Duration|DurationUnits|EffectiveStartDate|EffectiveEndDate|PersonId(SourceSystemId)|SourceSystemId|SourceSystemOwner');
--
--end if;
--close c_contract; 
       

      l_cnt := 0;
        for j in c_contract
        loop
--        
if l_cnt = 0 then
      UTL_FILE.put_line (
            l_file_handle, 'METADATA|Contract|AssignmentId|ContractId|ContractType|Description|Duration|DurationUnits|EffectiveStartDate|EffectiveEndDate|PersonId(SourceSystemId)|SourceSystemId|SourceSystemOwner');

end if;
           UTL_FILE.put_line (
            l_file_handle,
   j.MERGE  || '|' 
                                                                || j.Contract    || '|' 
                                                                || j.AssignmentId    || '|' 
                                                                || j.ContractId    || '|' 
                                                                || j.ContractType    || '|' 
                                                                || j.Description    || '|' 
                                                                || j.Duration  || '|' 
                                                                || j.DurationUnits  || '|' 
                                                                || j.EffectiveStartDate  || '|'
                                                                || j.EffectiveEndDate  || '|' 
                || j.pers_id  || '|' 
                                                                || j.SourceSystemId || '|' 
                                                                || j.SourceSystemOwner 
         );
           l_cnt := l_cnt +1;

        end loop;      

       l_cnt :=0 ;
        for i in c_ass_sup
        loop

if l_cnt = 0 then
      UTL_FILE.put_line (
              l_file_handle,'METADATA|AssignmentSupervisor|EffectiveStartDate|EffectiveEndDate|PrimaryFlag|AssignmentNumber|ManagerPersonNumber|ManagerAssignmentNumber|ManagerType|PersonId');
              
              

end if;
        --c_ass_sup  data
UTL_FILE.put_line (
            l_file_handle,
i.MERGE   || '|' 
   || i.ASSIGNMENTSUPERVISOR  || '|' 
   || i.EFFECTIVESTARTDATE  || '|' 
   || i.EFFECTIVEENDTDATE  || '|' 
   || i.PRIMARYFLAG  || '|' 
    || 'E'||i.PERSONNUMBER  || '|' 
   || i.MANAGERPERSONNUMBER  || '|' 
   || 'E'||i.MANAGERPERSONNUMBER  || '|'
   || i.MANAGERTYPE  || '|'
   || i.PERSONNUMBER  
    );


          
        l_cnt := l_cnt +1;
        end loop;  

UTL_FILE.fclose(l_file_handle);



-- UTL_FILE.fclose(l_file_handle);

EXCEPTION
      WHEN UTL_FILE.invalid_operation
      THEN
         dbms_output.put_line ( 
                            'Invalid Operation For ' || l_global_file); 
         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_path
      THEN
         dbms_output.put_line ( 
                            'Invalid Path For   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_mode
      THEN
         dbms_output.put_line ( 
                            'Invalid Mode For   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.invalid_filehandle
      THEN
         dbms_output.put_line ( 
                            'Invalid File Handle  ' || l_global_file);
         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.write_error
      THEN
         dbms_output.put_line (
                            'Invalid Write Error   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.read_error
      THEN
         dbms_output.put_line (
                            'Invalid Read  Error   ' || l_global_file);

         UTL_FILE.fclose(l_file_handle);
      WHEN UTL_FILE.internal_error
      THEN
         dbms_output.put_line (  'Internal Error');
         UTL_FILE.fclose(l_file_handle);
      WHEN OTHERS
      THEN
         dbms_output.put_line ( 'Error message due to others is: ' || SQLERRM);
         UTL_FILE.fclose(l_file_handle);

end process_Worker_dat;

procedure dat_to_zip  (x_errbuf OUT VARCHAR2, x_retcode OUT NUMBER,   p_zip_file_name in VARCHAR2 )
as
L_REQUEST_ID number;
l_seq number ;
l_zip_file_name varchar2(2000);
l_file1 BLOB;
l_zip BLOB;
l_file_GRADE BLOB;
l_file_JOB BLOB;
l_file_org BLOB;
l_file_WORKER BLOB;
l_grade_cnt number := 0;
l_job_cnt number := 0;
l_org_cnt number := 0;
l_worker_cnt number := 0;

begin
fnd_global.apps_initialize (0,20420,1);



l_zip_file_name := p_zip_file_name;





BEGIN
select count(1) into l_grade_cnt
FROM
    per_grades where trunc(CREATION_DATE) = trunc(sysdate);
    
    if l_grade_cnt > 0 then
    l_file_GRADE :=  xxqia_zip_util_pkg.file_to_blob ('Grade');
    xxqia_zip_util_pkg.add_file (l_zip,  'Grade.dat', l_file_GRADE);
fnd_file.put_line (fnd_file.LOG,'after Grade add_file ' );

END IF;

END;




begin
select count(1) into l_job_cnt

FROM
    per_positions              pp,
    per_position_definitions   ppd
WHERE
    pp.position_definition_id = ppd.position_definition_id
    and   trunc(ppd.CREATION_DATE) = trunc(sysdate);

if l_job_cnt >0 then

l_file_JOB :=  xxqia_zip_util_pkg.file_to_blob ('Job');
xxqia_zip_util_pkg.add_file (l_zip, 'Job.dat', l_file_JOB);

fnd_file.put_line (fnd_file.LOG,'after Job add_file ' );

end if;
end;

begin

select count(1) into l_org_cnt
FROM
        hr_organization_units     hou,
    hr_organization_information   hoi
WHERE
    hou.organization_id = hoi.organization_id
    and      hou.TYPE = '20'
   and     trunc( hoi.CREATION_DATE) = trunc(sysdate)     ;

if l_org_cnt > 0  then

l_file_ORG :=  xxqia_zip_util_pkg.file_to_blob ('Organization');
xxqia_zip_util_pkg.add_file (l_zip, 'Organization.dat', l_file_ORG);
fnd_file.put_line (fnd_file.LOG,'after Organization add_file ' );


end if;
end;

begin
select count(1) into l_worker_cnt

FROM
    per_all_people_f              papf,
    per_person_type_usages_f      pptu,
    per_person_types              ppt,
    per_all_assignments_f         paaf,
    per_periods_of_service        ppos,
    per_assignment_status_types   past,
    hr_all_positions_f            hapf,
    per_grades                    pg,
    per_jobs                      pj,
    hr_locations_all              hla
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
    AND papf.person_type_id = ppt.person_type_id
    AND ppt.system_person_type = 'EMP'
    AND papf.person_id = paaf.person_id
    AND paaf.person_id = pptu.person_id
    AND papf.person_id = ppos.person_id
    AND paaf.person_id = ppos.person_id
    AND paaf.assignment_status_type_id = past.assignment_status_type_id
    AND paaf.position_id = hapf.position_id (+)
    AND paaf.grade_id = pg.grade_id (+)
    AND paaf.job_id = pj.job_id (+)
    AND paaf.location_id = hla.location_id (+)
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate)))
order by  papf.person_id, to_char(paaf.effective_start_date, 'YYYY/MM/DD')    ,
    to_char(paaf.effective_end_date, 'YYYY/MM/DD') ;


if l_worker_cnt >0 then

l_file_WORKER :=  xxqia_zip_util_pkg.file_to_blob ('Worker');
xxqia_zip_util_pkg.add_file (l_zip,   'Worker.dat', l_file_WORKER);
fnd_file.put_line (fnd_file.LOG,'after Worker add_file ' );

end if;
end;








fnd_file.put_line (fnd_file.LOG,'after add_file ' );

xxqia_zip_util_pkg.finish_zip (l_zip);
fnd_file.put_line (fnd_file.LOG,'after finish_zip ');


fnd_file.put_line (fnd_file.LOG,'before save_zip '||l_zip_file_name||'.zip');

xxqia_zip_util_pkg.save_zip (l_zip, 'ZIP_OUT_PATH', l_zip_file_name||'.zip');
fnd_file.put_line (fnd_file.LOG,'aftter save_zip '||l_zip_file_name||'.zip');







      COMMIT;

end dat_to_zip;


function zip_to_blob(p_zip_file_name in VARCHAR2)
return blob
as

  l_output_directory varchar2 (30); 
    l_filename         varchar2 (255); 
    l_bfile            BFILE; 
    l_blob             BLOB; 
    dst_offset         number := 1; 
    src_offset         number := 1; 
    l_document         BLOB; 
    l_document_type    varchar2(100); 
begin
    fnd_file.put_line (fnd_file.LOG,'start zipping '|| p_zip_file_name||'.zip');

      l_bfile := Bfilename('ZIP_OUT_PATH', p_zip_file_name||'.zip');
          fnd_file.put_line (fnd_file.LOG,'after  zip out '|| p_zip_file_name||'.zip');

      dbms_lob.Fileopen(l_bfile); 
      dbms_lob.Createtemporary(l_blob, true); 
      dbms_lob.Loadblobfromfile (l_blob, l_bfile, dbms_lob.Getlength(l_bfile), 
      src_offset, dst_offset); 
                fnd_file.put_line (fnd_file.LOG,'after  Loadblobfromfile zip out '|| p_zip_file_name||'.zip');

      l_document := l_blob;
    fnd_file.put_line (fnd_file.LOG,utl_raw.cast_to_varchar2(utl_encode.base64_encode((l_document))));
    fnd_file.put_line (fnd_file.LOG,'end zipping ');
    
dbms_lob.close(l_bfile);
   return    l_document;



end;

PROCEDURE post_to_ucm(x_errbuf OUT VARCHAR2, x_retcode OUT NUMBER,p_zip_file_name in VARCHAR2)
as

soap_request varchar2(30000);
  soap_respond varchar2(30000);
  http_req utl_http.req;
  http_resp utl_http.resp;
  resp XMLType;
  l_usr_nm         varchar2(500);
  l_pwd            varchar2(500);
     l_wlt_path       varchar2(500);
   l_wlt_pwd        varchar2(500);
  i integer;
  l_value    varchar2(32767);
  l_ucm_content_id  VARCHAR2(200);
  l_file  blob;
  l_seq number;


begin


       fnd_file.put_line (fnd_file.LOG,
                                      'start'
                                    );
begin
select xxqia_fusion_integration.zip_to_blob(p_zip_file_name) into l_file from dual;

       fnd_file.put_line (fnd_file.LOG,
                                      'l_file '
                                    );
exception when others then 
null;

       fnd_file.put_line (fnd_file.LOG,
                                      'exception '||sqlerrm
                                    );
end;


if l_file is not null then 

       fnd_file.put_line (fnd_file.LOG,
                                      'l_file is not null  '
                                    );
  soap_request:= '<?xml version = "1.0" encoding = "UTF-8"?>
  <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ucm="http://www.oracle.com/UCM">
   <soapenv:Header/>
   <soapenv:Body>
      <ucm:GenericRequest webKey="cs">
         <ucm:Service IdcService="CHECKIN_UNIVERSAL">
            <ucm:User/>
            <ucm:Document>
               <ucm:Field name="dDocTitle">'||p_zip_file_name||' </ucm:Field>
               <ucm:Field name="dDocType">Document</ucm:Field>
               <ucm:Field name="dDocAuthor">HCM_IMPL</ucm:Field>
               <ucm:Field name="dSecurityGroup">FAFusionImportExport</ucm:Field>
               <ucm:Field name="dDocAccount">hcm$/dataloader$/import$</ucm:Field>
               <ucm:Field name="primaryFile">'||p_zip_file_name||'.zip</ucm:Field>
               <ucm:File href="'||p_zip_file_name||'.zip" name="primaryFile">
               <ucm:Contents>'||utl_raw.cast_to_varchar2(utl_encode.base64_encode((l_file)))||'</ucm:Contents>
               </ucm:File>
            </ucm:Document>
         </ucm:Service>
      </ucm:GenericRequest>
   </soapenv:Body>
</soapenv:Envelope>';

   l_usr_nm   := 'hcm_impl';
    l_pwd      := 'Welcome1';

    l_wlt_path := 'file:/home/oraprod/cert_fusion';
    l_wlt_pwd  := 'welcome123';
     fnd_file.put_line (fnd_file.LOG, 'after soap  ' );
dbms_output.put_line('after soap ');

utl_http.set_wallet(l_wlt_path, l_wlt_pwd);
fnd_file.put_line (fnd_file.LOG, 'after set_wallet  ' );
fnd_file.put_line (fnd_file.LOG,'after set_wallet ');

http_req:= utl_http.begin_request
            ( 'https://fa-esgr-saasfaprod1.fa.ocs.oraclecloud.com:443/idcws/GenericSoapPort'
            , 'POST'
            , 'HTTP/1.1'
            );
fnd_file.put_line (fnd_file.LOG,'after http_req ');
  fnd_file.put_line (fnd_file.LOG, 'after http_req  ' );




    utl_http.set_authentication(http_req, l_usr_nm, l_pwd);
    fnd_file.put_line (fnd_file.LOG,'after set_authentication ');
  fnd_file.put_line (fnd_file.LOG, 'after set_authentication  ' );

  utl_http.set_header(http_req, 'Content-Type', 'text/xml'); -- since we are dealing with plain text in XML documents
  utl_http.set_header(http_req, 'Content-Length', length(soap_request));
  utl_http.set_header(http_req, 'SOAPAction', ''); -- required to specify this is a SOAP communication
  utl_http.write_text(http_req, soap_request);
  http_resp:= utl_http.get_response(http_req);
  utl_http.read_text(http_resp, soap_respond);
  utl_http.end_response(http_resp);
fnd_file.put_line (fnd_file.LOG,soap_respond);
    fnd_file.put_line (fnd_file.LOG, 'soap_respond    '||soap_respond );

    fnd_file.put_line (fnd_file.LOG, 'soap_respond    '||substr(soap_respond ,instr(soap_respond , 'name="dDocName"')+16 , instr( substr(soap_respond ,instr(soap_respond , 'name="dDocName"')+16 , 40 ) ,'</' )-1 )    );

fnd_file.put_line (fnd_file.LOG,substr(soap_respond ,instr(soap_respond , 'name="dDocName"')+16 , instr( substr(soap_respond ,instr(soap_respond , 'name="dDocName"')+16 , 40 ) ,'</' )-1 )    );

  l_ucm_content_id := (substr(soap_respond ,instr(soap_respond , 'name="dDocName"')+16 , instr( substr(soap_respond ,instr(soap_respond , 'name="dDocName"')+16 , 40 ) ,'</' )-1 )    );

   fnd_file.put_line (fnd_file.LOG,'l_ucm_content_id '||l_ucm_content_id);

       fnd_file.put_line (fnd_file.LOG, 'before import_file_to_fusion    '    );
dbms_output.put_line('l_ucm_content_id '||l_ucm_content_id);
  xxqia_fusion_integration.import_file_to_fusion(l_ucm_content_id);
       fnd_file.put_line (fnd_file.LOG, 'after import_file_to_fusion    '    );

  end if;

exception when others then

dbms_output.put_line('eception '||sqlerrm);
fnd_file.put_line (fnd_file.LOG,'eception '||sqlerrm);

        fnd_file.put_line (fnd_file.LOG, 'eception '||sqlerrm    );

end;

procedure import_file_to_fusion(p_content_id in varchar2 )
as
soap_request varchar2(30000);
  soap_respond varchar2(30000);
  http_req utl_http.req;
  http_resp utl_http.resp;
  resp XMLType;
  l_usr_nm         varchar2(500);
  l_pwd            varchar2(500);
     l_wlt_path       varchar2(500);
   l_wlt_pwd        varchar2(500);
  i integer;
  l_value    varchar2(32767);

begin
       fnd_file.put_line (fnd_file.LOG, 'start import_file_to_fusion    ' ||p_content_id   );


  soap_request:= '<?xml version = "1.0" encoding = "UTF-8"?> 
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:typ="http://xmlns.oracle.com/apps/hcm/common/dataLoader/core/dataLoaderIntegrationService/types/">
   <soapenv:Header/>
   <soapenv:Body>
      <typ:importAndLoadData>
         <typ:ContentId>'|| p_content_id ||'</typ:ContentId>
<typ:Parameters>ImportMaximumErrors=100,LoadMaximumErrors=100,LoadConcurrentThreads=4,FileEncryption=NONE,DeleteSourceFile=N</typ:Parameters>
      </typ:importAndLoadData>
   </soapenv:Body>
</soapenv:Envelope>';


   l_usr_nm   := 'hcm_impl';
    l_pwd      := 'Welcome1';

    l_wlt_path := 'file:/home/oraprod/cert_fusion';
    l_wlt_pwd  := 'welcome123';

dbms_output.put_line('after soap ');
       fnd_file.put_line (fnd_file.LOG, ' after soap    '  );


utl_http.set_wallet(l_wlt_path, l_wlt_pwd);
fnd_file.put_line (fnd_file.LOG,'after set_wallet ');
        fnd_file.put_line (fnd_file.LOG, 'set_wallet    '  );


http_req:= utl_http.begin_request
            ( 
            'https://fa-esgr-saasfaprod1.fa.ocs.oraclecloud.com:443/hcmService/HCMDataLoader'
            --'https://fa-esgr-saasfaprod1.fa.ocs.oraclecloud.com:443/idcws/GenericSoapPort'
            , 'POST'
            , 'HTTP/1.1'
            );

fnd_file.put_line (fnd_file.LOG,'after http_req ');
        fnd_file.put_line (fnd_file.LOG, 'http_req    '  );



    utl_http.set_authentication(http_req, l_usr_nm, l_pwd);
    fnd_file.put_line (fnd_file.LOG,'after set_authentication ');
        fnd_file.put_line (fnd_file.LOG, 'set_authentication    '  );

  utl_http.set_header(http_req, 'Content-Type', 'text/xml'); -- since we are dealing with plain text in XML documents
  utl_http.set_header(http_req, 'Content-Length', length(soap_request));
  utl_http.set_header(http_req, 'SOAPAction', ''); -- required to specify this is a SOAP communication
  utl_http.write_text(http_req, soap_request);
  http_resp:= utl_http.get_response(http_req);
  utl_http.read_text(http_resp, soap_respond);
  utl_http.end_response(http_resp);

  fnd_file.put_line (fnd_file.LOG,soap_respond);
        fnd_file.put_line (fnd_file.LOG, 'soap_respond    '  );

exception when others then

fnd_file.put_line (fnd_file.LOG,'eception '||sqlerrm);
        fnd_file.put_line (fnd_file.LOG,'eception ' ||sqlerrm  );


end import_file_to_fusion;

procedure post_to_ucm_con_call(p_zip_file_name in VARCHAR2)
as
l_request_id number;
begin

--fnd_global.apps_initialize (0,20420,1);
     l_request_id := fnd_request.submit_request ( 
                            'XXQIA', 
                            'XXPOST_TO_UCM', 
                             'XXPOST TO UCM', 
                             sysdate, 
                            FALSE 
                            ,p_zip_file_name);

      COMMIT;

end post_to_ucm_con_call;


procedure dat_to_zip_call(p_zip_file_name out VARCHAR2,p_request_id out number)
as
l_request_id number;
l_zip_file_name  VARCHAR2(2000);
l_seq  number;

begin
l_seq  := qia_ws_seq.nextval;


l_zip_file_name:= 'HCM_'||'DATA'||'_'||l_seq;

fnd_file.put_line (fnd_file.LOG,' l_zip_file_name:=  '||l_zip_file_name);
fnd_global.apps_initialize (0,20420,1);
     l_request_id := fnd_request.submit_request ( 
                            'XXQIA', 
                            'xxdat_file_zip', 
                             'xxdat_file_zip', 
                             sysdate, 
                            FALSE 
--                            ,p_file_name
                            ,l_zip_file_name);

      COMMIT;


p_zip_file_name:= l_zip_file_name;
p_request_id:= l_request_id;
end dat_to_zip_call;


procedure MAIN (x_errbuf OUT VARCHAR2, x_retcode OUT NUMBER)
AS
l_request_id number;
l_zip_file_name varchar2(2000);
l_errbuf  varchar2(2000);
l_retcode number;

  lv_request_id       NUMBER;
  lc_phase            VARCHAR2(50);
  lc_status           VARCHAR2(50);
  lc_dev_phase        VARCHAR2(50);
  lc_dev_status       VARCHAR2(50);
  lc_message          VARCHAR2(50);
  l_req_return_status BOOLEAN;
  l_grade_cnt number:= 0;
  l_job_cnt  number := 0;

l_worker_cnt number :=0 ;
l_org_cnt number:=0;
BEGIN


begin

select count(1) into l_grade_cnt
FROM
    per_grades where trunc(CREATION_DATE) = trunc(sysdate);
    
    if l_grade_cnt > 0 then
    
--if p_file_name = 'Grade' then

xxqia_fusion_integration.process_grades_dat('Grade');

end if;
--    end if;

end;

begin
select count(1) into l_job_cnt

FROM
    per_positions              pp,
    per_position_definitions   ppd
WHERE
    pp.position_definition_id = ppd.position_definition_id
    and   trunc(ppd.CREATION_DATE) = trunc(sysdate);

if l_job_cnt >0 then
--if p_file_name = 'Job' then

xxqia_fusion_integration.process_Positions_dat('Job');

end if;
--end if;
end;

begin

select count(1) into l_org_cnt
FROM
        hr_organization_units     hou,
    hr_organization_information   hoi
WHERE
    hou.organization_id = hoi.organization_id
    and      hou.TYPE = '20'
   and     trunc( hoi.CREATION_DATE) = trunc(sysdate)     ;

if l_org_cnt > 0  then
--if p_file_name = 'Organization' then

xxqia_fusion_integration.process_Org_dat('Organization');

--end if;
end if;
end;

begin
select count(1) into l_worker_cnt

FROM
    per_all_people_f              papf,
    per_person_type_usages_f      pptu,
    per_person_types              ppt,
    per_all_assignments_f         paaf,
    per_periods_of_service        ppos,
    per_assignment_status_types   past,
    hr_all_positions_f            hapf,
    per_grades                    pg,
    per_jobs                      pj,
    hr_locations_all              hla
WHERE
    pptu.person_id = papf.person_id
    AND pptu.person_type_id = ppt.person_type_id
    AND papf.person_type_id = ppt.person_type_id
    AND ppt.system_person_type = 'EMP'
    AND papf.person_id = paaf.person_id
    AND paaf.person_id = pptu.person_id
    AND papf.person_id = ppos.person_id
    AND paaf.person_id = ppos.person_id
    AND paaf.assignment_status_type_id = past.assignment_status_type_id
    AND paaf.position_id = hapf.position_id (+)
    AND paaf.grade_id = pg.grade_id (+)
    AND paaf.job_id = pj.job_id (+)
    AND paaf.location_id = hla.location_id (+)
and ( (trunc(papf.LAST_UPDATE_DATE) = trunc(sysdate))
or(  trunc(paaf.LAST_UPDATE_DATE) = trunc(sysdate)))
and  (select max(ACTUAL_TERMINATION_DATE) from  per_periods_of_service   where PERIOD_OF_SERVICE_ID  = ppos.PERIOD_OF_SERVICE_ID ) is null
order by  papf.person_id, to_char(paaf.effective_start_date, 'YYYY/MM/DD')    ,
    to_char(paaf.effective_end_date, 'YYYY/MM/DD')
  ;


if l_worker_cnt >0 then
--if p_file_name = 'Worker' then

xxqia_fusion_integration.process_Worker_dat('Worker');

--end if;
end if;
end;


xxqia_fusion_integration.dat_to_zip_call (  l_zip_file_name,lv_request_id);


IF lv_request_id > 0 THEN
    LOOP
--
      --To make process execution to wait for 1st program to complete
      --
         l_req_return_status :=
            fnd_concurrent.wait_for_request (request_id      => lv_request_id
                                            ,INTERVAL        => 5 --interval Number of seconds to wait between checks
                                            ,max_wait        => 60 --Maximum number of seconds to wait for the request completion
                                             -- out arguments
                                            ,phase           => lc_phase
                                            ,STATUS          => lc_status
                                            ,dev_phase       => lc_dev_phase
                                            ,dev_status      => lc_dev_status
                                            ,message         => lc_message
                                            );                                                                                            
      EXIT
    WHEN UPPER (lc_phase) = 'COMPLETED' OR UPPER (lc_status) IN ('CANCELLED', 'ERROR', 'TERMINATED');
    END LOOP;
    
  IF UPPER (lc_phase) = 'COMPLETED' AND UPPER (lc_status) = 'NORMAL' THEN
      dbms_output.put_line( 'The XX_PROGRAM_1 request successful for request id: ' || lv_request_id);
      --
      --Submitting Second Concurrent Program XX_PROGRAM_2
      --
                  BEGIN
xxqia_fusion_integration.post_to_ucm_con_call(l_zip_file_name);
NULL;                                --             
      EXCEPTION
      WHEN OTHERS THEN
        dbms_output.put_line( 'OTHERS exception while submitting XX_PROGRAM_2: ' || SQLERRM);
      END;

end if;    
END IF;

END;

end xxqia_fusion_integration;
