The columns I used to cluster: total_qtrly_wages taxable_qtrly_wages taxable_qtrly_wages taxable_qtrly_wages oty_qtrly_estabs_chg oty_qtrly_estabs_pct_chg oty_total_qtrly_wages_chg oty_total_qtrly_wages_pct_chg oty_taxable_qtrly_wages_chg oty_taxable_qtrly_wages_pct_chg oty_qtrly_contributions_chg oty_qtrly_contributions_pct_chg oty_avg_wkly_wage_chg oty_avg_wkly_wage_pct_chg labor unemploy

1. Two cluster result: total cost 0.045, cost distance 0.004

+----------+-------------------+--------------------+------------------+--------------------+
|prediction|       avg(per_dem)|stddev_samp(per_dem)|      avg(per_gop)|stddev_samp(per_gop)|
+----------+-------------------+--------------------+------------------+--------------------+
|         1|    0.3845301690565| 0.23831380731737478|    0.549653181839|  0.2583720945136875|
|         0|0.31731623764103645|  0.1667422783676822|0.6295181835524905| 0.17239168671226704|
+----------+-------------------+--------------------+------------------+--------------------+

cluster 0 dem: 0.13688212927756654
cluster 0 gop: 0.7680608365019012
cluster 1 dem: 0.5
cluster 1 gop: 0.5

Three cluster result: total cost 0.024, cost distanct 0.003

+----------+------------------+--------------------+------------------+--------------------+
|prediction|      avg(per_dem)|stddev_samp(per_dem)|      avg(per_gop)|stddev_samp(per_gop)|
+----------+------------------+--------------------+------------------+--------------------+
|         1|   0.3845301690565| 0.23831380731737478|    0.549653181839|  0.2583720945136875|
|         2|  0.06651664647832|  0.0316565583466347|0.8960075409957999|0.049424028844921176|
|         0|0.3221766948341124|  0.1645625845583604|0.6243536611214185| 0.16985476581333828|
+----------+------------------+--------------------+------------------+--------------------+

cluster 0 dem: 0.49612403100775193
cluster 0 gop: 0.9534883720930233
cluster 1 dem: 0.5
cluster 1 gop: 1.0
cluster 2 dem: 0.0
cluster 2 gop: 1.0

dem: a county with more than 30% dem

gop: a county with more than 30% gop

It appears that the republicans are everywhere ¯\_(ツ)_/¯
