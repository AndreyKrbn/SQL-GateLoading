select
 s1.r_tid,
 [RN]=row_number() OVER(ORDER BY isnull(vl.TaskPriority,0), isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,'')), 
 [ПЛ+Направление]=isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,''), 
 [Приоритет]=isnull(vl.TaskPriority,0),
 [Дата и время отъезда] = cast(vl.WayListDate as smalldatetime),
 [Заявок]=max(qRequest.Заявок) over(partition by s1.r_tid),
 [План м3/ячеек]=cast(cast(SUM(isnull(qRequest.[План м3],0)) over(partition by s1.r_tid) as decimal(26,3)) as nvarchar(100))+N'/'+cast(sum(isnull(qRequest.[План ячеек],0)) over(partition by s1.r_tid) as nvarchar(50)),  
 [Занято м3/ячеек]=cast(cast(SUM(isnull(s1.[Занято м3],0)) over(partition by s1.r_tid) as decimal(26,3)) as nvarchar(100))+N'/'+cast(sum(isnull(s1.[Занято ячеек],0)) over(partition by s1.r_tid) as nvarchar(50)),
 [Кол-о ячеек%]=vl.WayListNumber+N' ('+cast(s1.[Занято ячеек] as nvarchar(50))+N'/'+cast([dbo].AllOutCells(s1.Gate_id) as nvarchar(50))+N')'+ case isnull(qRequest.Заявок,0) when 0 then '' else '*' end,
 [Док]=gt.NameRU + case s1.ExternalCode 
    when 'DOCKSTRING1' then N' (Сегмент 1)'
	when 'DOCKSTRING2' then N' (Сегмент 2)'
 	when 'DOCKSTRING3' then N' (Сегмент 3)'
end
from
(select
tz.ExternalCode,
l.Gate_id,
r.tid r_tid,
[Занято м3]=sum(isnull(st.Volume,0)),
[Занято ячеек]=count(distinct l.tid)
from Locations l (nolock)
 join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU='Нитки доков')
 join StorageObjects st (nolock) on (st.Location_id=l.tid)
 join Routes r (nolock) on (st.Route_id = r.tid)
 join Technozones as tz (nolock) on
 isnull(l.ComplectationArea_id, -1) = isnull(tz.ComplectationArea_id,-1)
 and l.StorageZone_id = tz.StorageZone_id
 and l.RouteZone_id = tz.RouteZone_id
group by
r.tid, l.Gate_id, tz.ExternalCode) s1
--обвес дока
 join Gates gt (nolock) on (gt.tid=s1.Gate_id and (gt.NameRU like '%OUT%'))
left join VisitorsLog vl (nolock) on (vl.Route_id = s1.r_tid) --!!!!

--Заказы
left join 
(select
   r.tid r_tid,
   b.Gate_id Gate_id,     
   [План м3]=isnull(sum((tbl.Quantity / mu.UnitKoeff) * mu.UnitVolume),0),   
   [План ячеек]=[dbo].PlanUsedCells(r.tid),
   [Заявок]=Count(distinct b.tid)      
--   [Строк]=Count(*)    
   from Routes r (nolock)    
   join hdr_delivery b (nolock) on r.tid = b.Route_id    
   join Transactions t (nolock) on b.Transaction_id = t.tid    
   join tbl_DeliveryRequestMaterials tbl (nolock) on t.ParentTransaction_id = tbl.Transaction_id    
   join MaterialUnits mu (nolock) on tbl.MaterialUnit_id = mu.tid
    group by r.tid, b.Gate_id) qRequest on (qRequest.r_tid  = s1.r_tid and qRequest.Gate_id = s1.Gate_id)	
ORDER BY isnull(vl.TaskPriority,0), isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,'')
