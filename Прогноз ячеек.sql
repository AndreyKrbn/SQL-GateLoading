

(select top 1  
   b.Route_id Route_id,
   [Прогноз ячеек] = sum(CEILING(isnull(sum(tbl.Quantity * mu.UnitVolume),0)/1.4)) over(partition by b.Gate_id, b.Route_id)
   from Routes r (readpast)    
   join hdr_delivery b (readpast) on r.tid = b.Route_id    
   join Transactions t (readpast) on b.Transaction_id = t.tid    
   join tbl_DeliveryRequestMaterials tbl (readpast) on t.ParentTransaction_id = tbl.Transaction_id    
   join MaterialUnits mu (readpast) on tbl.MaterialUnit_id = mu.tid   
   where b.Route_id = 117659      
    group by b.tid, b.Gate_id, b.Route_id)


	select [dbo].[PlanUsedCells](117659)