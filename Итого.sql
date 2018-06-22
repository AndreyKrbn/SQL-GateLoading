declare @columns nvarchar(max)
SET @columns = (
				select  '[' + cast(gt.NameRU as nvarchar(50)) + case tz.ExternalCode when 'DOCKSTRING1' then N' (������� 1)'
				                                                                    when 'DOCKSTRING2' then N' (������� 2)'
																					when 'DOCKSTRING3' then N' (������� 3)'
																					end +']'+ ', '
				from Locations l with(nolock)
                 join ComplectationAreas cma with(nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU='����� �����')
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


set @query = N'SELECT [� �/�] = '''',
                     [��+�����������] ='''',
					 [���������] = '''',
					 [���� � ����� �������] ='''',
					 [������] = '''',
					 [���� �3/�����] = '''',
					 [������ �3/�����] = '''',
					 ' + @columns + ' from 
            (
				select
				[���] = gt.NameRU + case s1.ExternalCode 
								when ''DOCKSTRING1'' then N'' (������� 1)''
								when ''DOCKSTRING2'' then N'' (������� 2)''
 								when ''DOCKSTRING3'' then N'' (������� 3)''
							end,
				[�����] = N'' (''+cast(isnull(s1.[������ ����� �� �� � ��������],0) as nvarchar(50))+N''/''+cast(isnull(s1.[������ �����],0) as nvarchar(50))+N''/''+cast([dbo].AllOutCells(gt.tid) as nvarchar(50))+N'')''
				from
				(select
				tz.ExternalCode,
				l.Gate_id,
				[������ ����� �� �� � ��������]=count(distinct case isnull(b.Route_id,0) when 0 then null else st.Location_id end),
				[������ �����]=count(distinct st.Location_id)
				from Locations l (nolock)
				 join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU=''����� �����'')
				 left join StorageObjects st (nolock) on (st.Location_id=l.tid)
				 left join Routes r (nolock) on (st.Route_id = r.tid)
				 left join hdr_delivery b (nolock) on r.tid = b.Route_id
				 join Technozones as tz (nolock) on
				 isnull(l.ComplectationArea_id, -1) = isnull(tz.ComplectationArea_id,-1)
				 and l.StorageZone_id = tz.StorageZone_id
				 and l.RouteZone_id = tz.RouteZone_id
				group by
				 l.Gate_id, tz.ExternalCode) s1
				--����� ����
				 right join Gates gt (nolock) on (gt.tid=s1.Gate_id and (gt.NameRU like ''%OUT%''))
				 where gt.NameRU like ''%OUT%''  and s1.ExternalCode is not null
			) x
            pivot 
            (
               max([�����])
               for [���] in (' + @columns + ')
		     ) p'
exec (@query)


