# Gold Layer config
from os import environ
from datetime import datetime

env = environ["ENV"]

source_table_dict = {'gam_advertisers': 'gam.advertisers_3379'
    , 'gam_orders': 'gam.orders_3379'
    , 'gam_deals': 'gam.deals_3379'
    , 'gam_ad_units': 'gam.ad_units_3379'
    , 'gam_line_items': 'gam.line_items_3379'
    , 'gam_network_backfill_imp': 'gam.NetworkBackfillImpressions_45577845'
                     }

dimension_notebooks = {
    'gam_common_dim_deals': {
        'dev': '/Repos/BalaMurugan_Amirthalingam@condenast.com/datawarehouse-cnhw/Dimension/Source_Code/Common_dim_deal/common_dim_deal',
        'prod': '/Repos/BalaMurugan_Amirthalingam@condenast.com/datawarehouse-cnhw/Dimension/Source_Code/Common_dim_deal/common_dim_deal'},
}
