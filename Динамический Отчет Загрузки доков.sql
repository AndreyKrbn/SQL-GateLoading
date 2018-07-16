IF object_id(N'PlanUsedCells', N'FN') IS NOT NULL
    DROP FUNCTION PlanUsedCells

exec (N'CREATE FUNCTION PlanUsedCells
(	
	@Param1 int
)
RETURNS int
AS
BEGIN	
	DECLARE @Result int
		
	select top 1     
     @Result = sum(CEILING(isnull(sum(vtbl.Quantity * vmu.UnitVolume),0)/1.4)) over(partition by vb.Gate_id, vb.Route_id)
   from Routes vr (nolock)    
   join hdr_delivery vb (nolock) on vr.tid = vb.Route_id    
   join Transactions vt (nolock) on vb.Transaction_id = vt.tid    
   join tbl_DeliveryRequestMaterials vtbl (nolock) on vt.ParentTransaction_id = vtbl.Transaction_id    
   join MaterialUnits vmu (nolock) on vtbl.MaterialUnit_id = vmu.tid   
   where vb.Route_id = @Param1      
    group by vb.tid, vb.Gate_id, vb.Route_id

	RETURN @Result
END')

IF object_id(N'AllOutCells', N'FN') IS NOT NULL
    DROP FUNCTION AllOutCells

exec (N'CREATE FUNCTION AllOutCells
(	
	@Param1 int
)
RETURNS int
AS
BEGIN	
	DECLARE @Result int
		
	select @Result=count(*) from Gates gt (nolock)
		 join Locations l (nolock) on (gt.tid=l.Gate_id)
		 join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU=''����� �����'')
		where gt.tid = @Param1 and l.IsBlockInput = 0		

	RETURN @Result
END')

declare @columns varchar(max)
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
				 join Gates gt with(nolock) on (gt.tid=l.Gate_id and gt.NameRU like 'OUT%')
				where l.IsBlockInput=0
				group by gt.NameRU, tz.ExternalCode
				order by cast (substring(gt.NameRU,4, len(gt.NameRU)) as int)
				FOR XML PATH(''))

			if @columns is null set @columns = ''
			if len(@columns)> 0 set @columns = @columns + '[OUT1000 (������� 1)]'

declare @query varchar(max)

set @query = N'SELECT [� �/�] = ROW_NUMBER() OVER (ORDER BY [���������], [���� � ����� �������] desc, [��+�����������]),
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
 [���� � ����� �������] = cast(isNull(vl.ArrivalDate,vl.WayListDate) as smalldatetime),
 [������]=PLDelivery.������,
 [���� �3/�����]=cast(cast(isnull(PLDelivery.[���� �3],0) as decimal(26,3)) as nvarchar(100))+N''/''+cast(isnull(PLDelivery.[���� �����],0) as nvarchar(50)),  
 [������ �3/�����]=cast(cast(SUM(isnull(cmpl.[������ �3],0)) over(partition by cmpl.r_tid) as decimal(26,3)) as nvarchar(100))+N''/''+cast(sum(isnull(cmpl.[������ �����],0)) over(partition by cmpl.r_tid) as nvarchar(50)),
 [���-� �����%]=vl.WayListNumber+N'' (''+cast(isnull(cmpl.[������ �����],0) as nvarchar(50))+N''/''+cast([dbo].AllOutCells(isnull(cmpl.Gate_id,PLDelivery.Gate_id)) as nvarchar(50))+N'')''+ case (select top 1 Gate_id from Routes where tid=PLDelivery.r_tid) when isnull(cmpl.Gate_id,PLDelivery.Gate_id) then ''*'' else '''' end,
 [���]=isnull(gtl.NameRU + case tz�.ExternalCode 
    when ''SEGMENTCOMP1'' then N'' (������� 1)''
	when ''SEGMENTCOMP2'' then N'' (������� 2)''
 	when ''SEGMENTCOMP3'' then N'' (������� 3)''
end,gtd.NameRU + case tzd.ExternalCode 
    when ''SEGMENTCOMP1'' then N'' (������� 1)''
	when ''SEGMENTCOMP2'' then N'' (������� 2)''
 	when ''SEGMENTCOMP3'' then N'' (������� 3)''
end)

from
(select
l.Gate_id, --����� ������������
r.tid r_tid,
[������ �3]=sum(isnull(st.Volume,0)),
[������ �����]=count(distinct l.tid)
from Locations l (nolock)
 join ComplectationAreas cma (nolock) on (cma.tid = l.ComplectationArea_id and cma.NameRU=''����� �����'')
 join StorageObjects st (nolock) on (st.Location_id=l.tid)
 join Routes r (nolock) on (st.Route_id = r.tid)
where l.IsBlockInput=0
group by
r.tid, l.Gate_id) cmpl
--����� ���� ����� ������������
 join Gates gtl (nolock) on (gtl.tid=cmpl.Gate_id and (gtl.NameRU like ''%OUT%'')) -- ������ ����� ������������
 join Technozones as tz� (nolock) on	tz�.tid = gtl.TechnoZone_id 
--������
right join 
(select
   r.tid r_tid,
   b.Gate_id Gate_id, --������       
   [���� �3]=isnull(sum((tbl.Quantity / case mu.UnitKoeff when 0 then 1 else mu.UnitKoeff end) * mu.UnitVolume),0),   
   [���� �����]=[dbo].PlanUsedCells(r.tid),
   [������]=Count(distinct b.tid)         
   from Routes r (nolock)    
   join hdr_delivery b (nolock) on r.tid = b.Route_id    
   join Transactions t (nolock) on b.Transaction_id = t.tid    
   join tbl_DeliveryRequestMaterials tbl (nolock) on t.ParentTransaction_id = tbl.Transaction_id    
   join MaterialUnits mu (nolock) on tbl.MaterialUnit_id = mu.tid
   where
   b.IsShipped = 0
    group by r.tid, b.Gate_id) PLDelivery on (PLDelivery.r_tid  = cmpl.r_tid)

--����� �������� �� �������
 join Gates gtd (nolock) on (gtd.tid=PLDelivery.Gate_id and (gtd.NameRU like ''%OUT%'')) -- ������ ������
 join VisitorsLog vl (nolock) on (vl.Route_id = PLDelivery.r_tid)	
 join Technozones as tzd (nolock) on	tzd.tid = gtd.TechnoZone_id
			) x
            pivot 
            (
               max([���-� �����%])
               for [���] in (' + @columns + ')
		     ) p
			 where [������] is not null				
			 ORDER BY [���������], [���� � ����� �������] desc, [��+�����������]'

exec (@query)

go
-----------------------------------
declare @columns varchar(max)
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
				 join Gates gt with(nolock) on (gt.tid=l.Gate_id and gt.NameRU like 'OUT%')
				where l.IsBlockInput=0
				group by gt.NameRU, tz.ExternalCode
				order by cast (substring(gt.NameRU,4, len(gt.NameRU)) as int)
				FOR XML PATH(''))

			if @columns is null set @columns = ''
			if len(@columns)> 0 set @columns = @columns + '[OUT1000 (������� 1)]'


declare @query nvarchar(max)
declare @totalsel int

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
				[�����] = N'' (''+cast(isnull(s1.[������ ����� �� �� � ��������],0) as nvarchar(50))+N''/''+cast(isnull(s1.[������ �����],0) as nvarchar(50))+N''/''+cast([dbo].AllOutCells(gt.tid) as nvarchar(50))+N'')''+N'' (Free= ''+cast([dbo].AllOutCells(gt.tid)-isnull(s1.[������ �����],0) as nvarchar(50))+N'')''
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
				 where l.IsBlockInput=0				 
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
