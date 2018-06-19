select
 [RN] = ROW_NUMBER() OVER (ORDER BY vl.TaskPriority),
 [ПЛ+Направление]=vl.WayListNumber+N' - '+vl.direction, 
 [Приоритет]=vl.TaskPriority,
 [Дата и время отъезда] = cast(vl.WayListDate as smalldatetime),
 qRequest.[Заявок],
 qRequest.[План м3/ячеек],  
 s1.[Занято м3/ячеек],
 [Кол-о ячеек] =vl.WayListNumber+N' ('+s1.[Кол-о ячеек]+N'/'+cast(tc.tc as nvarchar(50))+N')',
 [Док] = gt.NameRU
from
(select
l.Gate_id,
r.tid r_tid,
[Кол-о ячеек] = cast(count(distinct l.tid) as nvarchar(50)),
[Занято м3/ячеек]=cast(cast(sum(st.Volume) as numeric(18,2)) as nvarchar(100))+N'/'+cast(count(distinct l.tid) as nvarchar(50))
from Locations l with(nolock)
inner join ComplectationAreas cma with(nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU='Нитки доков')
inner join StorageObjects st with(nolock) on (st.Location_id=l.tid)
inner join Routes r with(nolock) on (st.Route_id = r.tid)
group by
r.tid, l.Gate_id) s1

--обвес дока
inner join Gates gt with(nolock) on (gt.tid=s1.Gate_id and gt.NameRU like '%OUT%')
left join VisitorsLog vl with(nolock) on (vl.Route_id = s1.r_tid) --!!!!

inner join (select count(*) tc, gt.tid from Gates gt with(nolock)
inner join Locations l with(nolock) on (gt.tid=l.Gate_id)
inner join ComplectationAreas cma with(nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU='Нитки доков')
group by gt.tid
) tc on (tc.tid  = s1.Gate_id)

--Заказы
left join 
(select
   r.tid r_tid,
   b.Gate_id Gate_id,     
   [План м3/ячеек] = cast(cast(sum(tbl.Quantity * mu.UnitVolume) as numeric(18,2)) as nvarchar(100))+N'/'+cast(CEILING(sum(tbl.Quantity * mu.UnitVolume)/1.4) as nvarchar(50)),   
   [Заявок] = Count(*)    
   from Routes r with(nolock)    
   join hdr_delivery b with(nolock) on r.tid = b.Route_id    
   join Transactions t with(nolock) on b.Transaction_id = t.tid    
   join tbl_DeliveryRequestMaterials tbl with(nolock) on t.ParentTransaction_id = tbl.Transaction_id    
   join MaterialUnits mu with(nolock) on tbl.MaterialUnit_id = mu.tid
    group by r.tid, b.Gate_id) qRequest on (qRequest.r_tid  = s1.r_tid 
	and qRequest.Gate_id = s1.Gate_id
	)
	where vl.WayListNumber+N' - '+vl.direction = 'РЕЛ-000012598 - филиал Новосибирск'
order by vl.TaskPriority
