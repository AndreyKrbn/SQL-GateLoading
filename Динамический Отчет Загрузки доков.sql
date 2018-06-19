declare @columns nvarchar(450)
SET @columns = (
				select  '[' + cast(gt.NameRU as varchar(50)) + ']' + ', '
				from Locations l with(nolock)
                inner join ComplectationAreas cma with(nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU='����� �����')
				inner join Gates gt with(nolock) on ((gt.tid=l.Gate_id and gt.NameRU like 'OUT%') or gt.NameRU = 'OUT1000')
				group by gt.NameRU
				order by cast (substring(gt.NameRU,4, len(gt.NameRU)) as int)
				FOR XML PATH(''))

			if @columns is null set @columns = ''
			if len(@columns)> 0 set @columns = Left(@columns,len(@columns) - 1)

declare @query nvarchar(max)
set @query = 'SELECT [� �/�] = ROW_NUMBER() OVER (ORDER BY [���������], [��+�����������]),
                     [��+�����������],
					 [���������],
					 [���� � ����� �������],
					 [������],
					 [���� �3/�����],
					 [������ �3/�����],
					 ' + @columns + ' from 
            (
				select
				 [��+�����������]=isnull(vl.WayListNumber,'''')+N'' - ''+isnull(vl.direction,''''), 
				 [���������]=isnull(vl.TaskPriority,0),
				 [���� � ����� �������] = cast(vl.WayListDate as smalldatetime),
				 [������] = max(qRequest.������) over(partition by s1.r_tid),
				 [���� �3/�����] = cast(cast(SUM(isnull(qRequest.[���� �3],0)) over(partition by s1.r_tid) as decimal(26,3)) as nvarchar(100))+N''/''+cast(sum(isnull(qRequest.[������ �����],0)) over(partition by s1.r_tid) as nvarchar(50)),  
				 [������ �3/�����] = cast(cast(SUM(isnull(s1.[������ �3],0)) over(partition by s1.r_tid) as decimal(26,3)) as nvarchar(100))+N''/''+cast(sum(isnull(s1.[������ �����],0)) over(partition by s1.r_tid) as nvarchar(50)),
				 [���-� �����%] =vl.WayListNumber+N'' (''+s1.[���-� �����]+N''/''+cast(tc.tc as nvarchar(50))+N'')''+ case isnull(qRequest.������,0) when 0 then '''' else ''*'' end,
				 [���] = gt.NameRU 
				from
				(select
				l.Gate_id,
				r.tid r_tid,
				[���-� �����] = cast(isnull(count(distinct l.tid),0) as nvarchar(50)),
				[������ �3] =sum(isnull(st.Volume,0)),
				[������ �����] =count(distinct l.tid)
				from Locations l (nolock)
				inner join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU=''����� �����'')
				inner join StorageObjects st (nolock) on (st.Location_id=l.tid)
				inner join Routes r (nolock) on (st.Route_id = r.tid)
				group by
				r.tid, l.Gate_id) s1

				--����� ����
				inner join Gates gt (nolock) on (gt.tid=s1.Gate_id and (gt.NameRU like ''%OUT%''))
				left join VisitorsLog vl (nolock) on (vl.Route_id = s1.r_tid) --!!!!

				inner join (select count(*) tc, gt.tid from Gates gt with(nolock)
				inner join Locations l (nolock) on (gt.tid=l.Gate_id)
				inner join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU=''����� �����'')
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
				   from Routes r (nolock)    
				   join hdr_delivery b (nolock) on r.tid = b.Route_id    
				   join Transactions t (nolock) on b.Transaction_id = t.tid    
				   join tbl_DeliveryRequestMaterials tbl (nolock) on t.ParentTransaction_id = tbl.Transaction_id    
				   join MaterialUnits mu (nolock) on tbl.MaterialUnit_id = mu.tid
					group by r.tid, b.Gate_id) qRequest on (qRequest.r_tid  = s1.r_tid 
					and qRequest.Gate_id = s1.Gate_id
					)
           ) x
            pivot 
            (
               max([���-� �����%])
               for [���] in (' + @columns + ')
		     ) p
			 where [������] is not null				
			 ORDER BY [���������], [��+�����������]'
 exec(@query)