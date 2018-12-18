select distinct c.sku, c.venture_code, c.id_catalog_config, c.image_url, c.activated_at, c.brand, c.category_id, c.created_at, c.short_description
from edw.catalog_config as c
left join edw.catalog_simple as s
on c.id_catalog_config = s.fk_catalog_config
where c.venture_code = '{}'
and s.is_visible = 1
and s.stock > 0
and c.activated_at is not Null
and c.created_at >= '{}'
and c.created_at <= '{}'
