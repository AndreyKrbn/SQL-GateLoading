select
 PLDelivery.r_tid,
 cmpl.r_tid,
 [RN]=row_number() OVER(ORDER BY isnull(vl.TaskPriority,0), isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,'')), 
 [��+�����������]=isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,''), 
 [���������]=isnull(vl.TaskPriority,0),
 [���� � ����� �������] = cast(isNull(vl.ArrivalDate,vl.WayListDate) as smalldatetime),
 [������]=PLDelivery.������,
 [���� �3/�����]=cast(cast(isnull(PLDelivery.[���� �3],0) as decimal(26,3)) as nvarchar(100))+N'/'+cast(isnull(PLDelivery.[���� �����],0) as nvarchar(50)),  
 [������ �3/�����]=cast(cast(SUM(isnull(cmpl.[������ �3],0)) over(partition by cmpl.r_tid) as decimal(26,3)) as nvarchar(100))+N'/'+cast(sum(isnull(cmpl.[������ �����],0)) over(partition by cmpl.r_tid) as nvarchar(50)),
 [���-� �����%]=vl.WayListNumber+N' ('+cast(isnull(cmpl.[������ �����],0) as nvarchar(50))+N'/'+cast([dbo].AllOutCells(isnull(cmpl.Gate_id,PLDelivery.Gate_id)) as nvarchar(50))+N')'+ case (select top 1 Gate_id from Routes where tid=PLDelivery.r_tid) when isnull(cmpl.Gate_id,PLDelivery.Gate_id) then '*' else '' end,
 [���]=isnull(gtl.NameRU + case tz�.ExternalCode 
    when 'SEGMENTCOMP1' then N' (������� 1)'
	when 'SEGMENTCOMP2' then N' (������� 2)'
 	when 'SEGMENTCOMP3' then N' (������� 3)'
end,gtd.NameRU + case tzd.ExternalCode 
    when 'SEGMENTCOMP1' then N' (������� 1)'
	when 'SEGMENTCOMP2' then N' (������� 2)'
 	when 'SEGMENTCOMP3' then N' (������� 3)'
end)

from
(select
l.Gate_id, --����� ������������
r.tid r_tid,
[������ �3]=sum(isnull(st.Volume,0)),
[������ �����]=count(distinct l.tid)
from Locations l (nolock)
 join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU='����� �����')
 join StorageObjects st (nolock) on (st.Location_id=l.tid)
 join Routes r (nolock) on (st.Route_id = r.tid)
where l.IsBlockInput=0
group by
r.tid, l.Gate_id) cmpl
--����� ���� ����� ������������
 join Gates gtl (nolock) on (gtl.tid=cmpl.Gate_id and (gtl.NameRU like '%OUT%')) -- ������ ����� ������������
 join Technozones as tz� (nolock) on	tz�.tid = gtl.TechnoZone_id 
--������
right join 
(select
   r.tid r_tid,
   b.Gate_id Gate_id, --��������       
   [���� �3]=isnull(sum((tbl.Quantity / case mu.UnitKoeff when 0 then 1 else mu.UnitKoeff end) * mu.UnitVolume),0),   
   [���� �����]=[dbo].PlanUsedCells(r.tid),
   [������]=Count(distinct b.tid)      
--   [�����]=Count(*)    
   from Routes r (nolock)    
   join hdr_delivery b (nolock) on r.tid = b.Route_id    
   join Transactions t (nolock) on b.Transaction_id = t.tid    
   join tbl_DeliveryRequestMaterials tbl (nolock) on t.ParentTransaction_id = tbl.Transaction_id    
   join MaterialUnits mu (nolock) on tbl.MaterialUnit_id = mu.tid
   where
   b.IsShipped = 0
   -- not exists (select  top 1 1 from hdr_DeliveryShipped where hdr_DeliveryShipped.Transaction_id = b.Transaction_id) 
    group by r.tid, b.Gate_id) PLDelivery on (PLDelivery.r_tid  = cmpl.r_tid)

--����� �������� �� ��������
 join Gates gtd (nolock) on (gtd.tid=PLDelivery.Gate_id and (gtd.NameRU like '%OUT%')) -- ������ ������
 join VisitorsLog vl (nolock) on (vl.Route_id = PLDelivery.r_tid)	
 join Technozones as tzd (nolock) on tzd.tid = gtd.TechnoZone_id

ORDER BY isnull(vl.TaskPriority,0)
