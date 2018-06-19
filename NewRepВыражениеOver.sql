select
 s1.r_tid,
 [RN] = row_number() OVER(ORDER BY isnull(vl.TaskPriority,0), isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,'')), 
 [��+�����������]=isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,''), 
 [���������]=isnull(vl.TaskPriority,0),
 [���� � ����� �������] = cast(vl.WayListDate as smalldatetime),
 [������] = max(qRequest.������) over(partition by s1.r_tid),
 [���� �3/�����] = cast(cast(SUM(isnull(qRequest.[���� �3],0)) over(partition by s1.r_tid) as decimal(26,3)) as nvarchar(100))+N'/'+cast(sum(isnull(qRequest.[������ �����],0)) over(partition by s1.r_tid) as nvarchar(50)),  
 [������ �3/�����] = cast(cast(SUM(isnull(s1.[������ �3],0)) over(partition by s1.r_tid) as decimal(26,3)) as nvarchar(100))+N'/'+cast(sum(isnull(s1.[������ �����],0)) over(partition by s1.r_tid) as nvarchar(50)),
 [���-� �����%] =vl.WayListNumber+N' ('+s1.[���-� �����]+N'/'+cast(tc.tc as nvarchar(50))+N')'+ case isnull(qRequest.������,0) when 0 then '' else '*' end,
 [���] = gt.NameRU 
from
(select
l.Gate_id,
r.tid r_tid,
[���-� �����] = cast(isnull(count(distinct l.tid),0) as nvarchar(50)),
[������ �3] =sum(isnull(st.Volume,0)),
[������ �����] =count(distinct l.tid)
from Locations l (readpast)
inner join ComplectationAreas cma (readpast) on (cma.tid = l.ComplectationArea_id and cma.NameRU='����� �����')
inner join StorageObjects st (readpast) on (st.Location_id=l.tid)
inner join Routes r (readpast) on (st.Route_id = r.tid)
group by
r.tid, l.Gate_id) s1

--����� ����
inner join Gates gt (readpast) on (gt.tid=s1.Gate_id and (gt.NameRU like '%OUT%'))
left join VisitorsLog vl (readpast) on (vl.Route_id = s1.r_tid) --!!!!

inner join (select count(*) tc, gt.tid from Gates gt with(readpast)
inner join Locations l (readpast) on (gt.tid=l.Gate_id)
inner join ComplectationAreas cma (readpast) on (cma.tid = l.ComplectationArea_id and cma.NameRU='����� �����')
group by gt.tid
) tc on (tc.tid  = s1.Gate_id)

--������
left join 
(select
   r.tid r_tid,
   b.Gate_id Gate_id,     
   [���� �3] = isnull(sum((tbl.Quantity / mu.UnitKoeff) * mu.UnitVolume),0),
   [������ �����] = isnull(CEILING(sum(tbl.Quantity * mu.UnitVolume)/1.4),0),
   [������] = Count(distinct b.tid),      
   [�����] = Count(*)    
   from Routes r (readpast)    
   join hdr_delivery b (readpast) on r.tid = b.Route_id    
   join Transactions t (readpast) on b.Transaction_id = t.tid    
   join tbl_DeliveryRequestMaterials tbl (readpast) on t.ParentTransaction_id = tbl.Transaction_id    
   join MaterialUnits mu (readpast) on tbl.MaterialUnit_id = mu.tid
   --join MaterialUnits as mu with (readpast) on mu.tid = case when dbo.bor_MaterialKoeff(b.MaterialUnit_id)>0 then b.MaterialUnit_id  else a.MaterialUnit_id  end   
    group by r.tid, b.Gate_id) qRequest on (qRequest.r_tid  = s1.r_tid 
	and qRequest.Gate_id = s1.Gate_id
	)
ORDER BY isnull(vl.TaskPriority,0), isnull(vl.WayListNumber,'')+N' - '+isnull(vl.direction,'')
