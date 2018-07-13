select
 PLDelivery.r_tid,
 cmpl.r_tid,
 [RN]=row_number() OVER(ORDER BY isnull(vl.TaskPriority,0), isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,'')), 
 [ПЛ+Направление]=isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,''), 
 [Приоритет]=isnull(vl.TaskPriority,0),
 [Дата и время отъезда] = cast(isNull(vl.ArrivalDate,vl.WayListDate) as smalldatetime),
 [Заявок]=PLDelivery.Заявок,
 [План м3/ячеек]=cast(cast(isnull(PLDelivery.[План м3],0) as decimal(26,3)) as nvarchar(100))+N'/'+cast(isnull(PLDelivery.[План ячеек],0) as nvarchar(50)),  
 [Занято м3/ячеек]=cast(cast(SUM(isnull(cmpl.[Занято м3],0)) over(partition by cmpl.r_tid) as decimal(26,3)) as nvarchar(100))+N'/'+cast(sum(isnull(cmpl.[Занято ячеек],0)) over(partition by cmpl.r_tid) as nvarchar(50)),
 [Кол-о ячеек%]=vl.WayListNumber+N' ('+cast(isnull(cmpl.[Занято ячеек],0) as nvarchar(50))+N'/'+cast([dbo].AllOutCells(isnull(cmpl.Gate_id,PLDelivery.Gate_id)) as nvarchar(50))+N')'+ case (select top 1 Gate_id from Routes where tid=PLDelivery.r_tid) when isnull(cmpl.Gate_id,PLDelivery.Gate_id) then '*' else '' end,
 [Док]=isnull(gtl.NameRU + case tzс.ExternalCode 
    when 'SEGMENTCOMP1' then N' (Сегмент 1)'
	when 'SEGMENTCOMP2' then N' (Сегмент 2)'
 	when 'SEGMENTCOMP3' then N' (Сегмент 3)'
end,gtd.NameRU + case tzd.ExternalCode 
    when 'SEGMENTCOMP1' then N' (Сегмент 1)'
	when 'SEGMENTCOMP2' then N' (Сегмент 2)'
 	when 'SEGMENTCOMP3' then N' (Сегмент 3)'
end)

from
(select
l.Gate_id, --ячеек комплектации
r.tid r_tid,
[Занято м3]=sum(isnull(st.Volume,0)),
[Занято ячеек]=count(distinct l.tid)
from Locations l (nolock)
 join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU='Нитки доков')
 join StorageObjects st (nolock) on (st.Location_id=l.tid)
 join Routes r (nolock) on (st.Route_id = r.tid)
where l.IsBlockInput=0
group by
r.tid, l.Gate_id) cmpl
--обвес дока ячеек комплектации
 join Gates gtl (nolock) on (gtl.tid=cmpl.Gate_id and (gtl.NameRU like '%OUT%')) -- ворота ячеек комплектации
 join Technozones as tzс (nolock) on	tzс.tid = gtl.TechnoZone_id 
--Заказы
right join 
(select
   r.tid r_tid,
   b.Gate_id Gate_id, --поставки       
   [План м3]=isnull(sum((tbl.Quantity / case mu.UnitKoeff when 0 then 1 else mu.UnitKoeff end) * mu.UnitVolume),0),   
   [План ячеек]=[dbo].PlanUsedCells(r.tid),
   [Заявок]=Count(distinct b.tid)      
--   [Строк]=Count(*)    
   from Routes r (nolock)    
   join hdr_delivery b (nolock) on r.tid = b.Route_id    
   join Transactions t (nolock) on b.Transaction_id = t.tid    
   join tbl_DeliveryRequestMaterials tbl (nolock) on t.ParentTransaction_id = tbl.Transaction_id    
   join MaterialUnits mu (nolock) on tbl.MaterialUnit_id = mu.tid
   where
   b.IsShipped = 0
   -- not exists (select  top 1 1 from hdr_DeliveryShipped where hdr_DeliveryShipped.Transaction_id = b.Transaction_id) 
    group by r.tid, b.Gate_id) PLDelivery on (PLDelivery.r_tid  = cmpl.r_tid)

--Обвес путевого по поставки
 join Gates gtd (nolock) on (gtd.tid=PLDelivery.Gate_id and (gtd.NameRU like '%OUT%')) -- ворота заказа
 join VisitorsLog vl (nolock) on (vl.Route_id = PLDelivery.r_tid)	
 join Technozones as tzd (nolock) on tzd.tid = gtd.TechnoZone_id

ORDER BY isnull(vl.TaskPriority,0)
