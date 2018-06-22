declare @columns nvarchar(max)
SET @columns = (
				select  '[' + cast(gt.NameRU as nvarchar(50)) + case tz.ExternalCode when 'DOCKSTRING1' then N' (Сегмент 1)'
				                                                                    when 'DOCKSTRING2' then N' (Сегмент 2)'
																					when 'DOCKSTRING3' then N' (Сегмент 3)'
																					end +']'+ ', '
				from Locations l with(nolock)
                 join ComplectationAreas cma with(nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU='Нитки доков')
				 join Technozones as tz (nolock) on
					isnull(l.ComplectationArea_id, -1) = isnull(tz.ComplectationArea_id,-1)
					and l.StorageZone_id = tz.StorageZone_id
					and l.RouteZone_id = tz.RouteZone_id
				 join Gates gt with(nolock) on ((gt.tid=l.Gate_id and gt.NameRU like 'OUT%') or gt.NameRU = 'OUT1000')
				group by gt.NameRU, tz.ExternalCode
				order by cast (substring(gt.NameRU,4, len(gt.NameRU)) as int)
				FOR XML PATH(''))

			if @columns is null set @columns = ''
			if len(@columns)> 0 set @columns = Left(@columns,len(@columns) - 1)


declare @query nvarchar(max)


set @query = N'SELECT [№ п/п] = '''',
                     [ПЛ+Направление] ='''',
					 [Приоритет] = '''',
					 [Дата и время отъезда] ='''',
					 [Заявок] = '''',
					 [План м3/ячеек] = '''',
					 [Занято м3/ячеек] = '''',
					 ' + @columns + ' from 
            (
				select
				[Док] = gt.NameRU + case s1.ExternalCode 
								when ''DOCKSTRING1'' then N'' (Сегмент 1)''
								when ''DOCKSTRING2'' then N'' (Сегмент 2)''
 								when ''DOCKSTRING3'' then N'' (Сегмент 3)''
							end,
				[Итого] = N'' (''+cast(isnull(s1.[Занято ячеек по ПЛ с заказами],0) as nvarchar(50))+N''/''+cast(isnull(s1.[Занято ячеек],0) as nvarchar(50))+N''/''+cast([dbo].AllOutCells(gt.tid) as nvarchar(50))+N'')''
				from
				(select
				tz.ExternalCode,
				l.Gate_id,
				[Занято ячеек по ПЛ с заказами]=count(distinct case isnull(b.Route_id,0) when 0 then null else st.Location_id end),
				[Занято ячеек]=count(distinct st.Location_id)
				from Locations l (nolock)
				 join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU=''Нитки доков'')
				 left join StorageObjects st (nolock) on (st.Location_id=l.tid)
				 left join Routes r (nolock) on (st.Route_id = r.tid)
				 left join hdr_delivery b (nolock) on r.tid = b.Route_id
				 join Technozones as tz (nolock) on
				 isnull(l.ComplectationArea_id, -1) = isnull(tz.ComplectationArea_id,-1)
				 and l.StorageZone_id = tz.StorageZone_id
				 and l.RouteZone_id = tz.RouteZone_id
				group by
				 l.Gate_id, tz.ExternalCode) s1
				--обвес дока
				 right join Gates gt (nolock) on (gt.tid=s1.Gate_id and (gt.NameRU like ''%OUT%''))
				 where gt.NameRU like ''%OUT%''  and s1.ExternalCode is not null
			) x
            pivot 
            (
               max([Итого])
               for [Док] in (' + @columns + ')
		     ) p'
exec (@query)


