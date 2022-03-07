from pipelines.utils import run_local
from functools import reduce
from pipelines.datasets.br_ibge_ipca15 import (
    br_ibge_ipca15_mes_brasil,
    br_ibge_ipca15_mes_categoria_rm,
    br_ibge_ipca15_mes_categoria_municipio,
    br_ibge_ipca15_mes_categoria_brasil,
)

from pipelines.datasets.br_ibge_ipca import (
    br_ibge_ipca_mes_brasil,
    br_ibge_ipca_mes_categoria_rm,
    br_ibge_ipca_mes_categoria_municipio,
    br_ibge_ipca_mes_categoria_brasil,
)

from pipelines.datasets.br_ibge_inpc import (
    br_ibge_inpc_mes_brasil,
    br_ibge_inpc_mes_categoria_rm,
    br_ibge_inpc_mes_categoria_municipio,
    br_ibge_inpc_mes_categoria_brasil,
)

from pipelines.datasets.br_cvm_administradores_carteira import br_cvm_adm_car_res, br_cvm_adm_car_pes_fis, br_cvm_adm_car_pes_jur
from pipelines.datasets.br_cvm_oferta_publica_distribuicao import br_cvm_ofe_pub_dis_dia
# from pipelines.datasets.br_ms_vacinacao_covid19 import (download_data, br_ms_vacinacao_covid19_microdados,br_ms_vacinacao_covid19_microdados_estabelecimento,
# br_ms_vacinacao_covid19_microdados_paciente, br_ms_vacinacao_covid19_microdados_vacinacao)

# flows_vacinacao = [download_data, br_ms_vacinacao_covid19_microdados,br_ms_vacinacao_covid19_microdados_estabelecimento,
# br_ms_vacinacao_covid19_microdados_paciente, br_ms_vacinacao_covid19_microdados_vacinacao]

flows_inpc = [br_ibge_inpc_mes_brasil,
    br_ibge_inpc_mes_categoria_rm,
    br_ibge_inpc_mes_categoria_municipio,
    br_ibge_inpc_mes_categoria_brasil]

flows_ipca = [
    br_ibge_ipca_mes_brasil,
    br_ibge_ipca_mes_categoria_rm,
    br_ibge_ipca_mes_categoria_municipio,
    br_ibge_ipca_mes_categoria_brasil]

flows_ipca15 = [br_ibge_ipca15_mes_brasil,
    br_ibge_ipca15_mes_categoria_rm,
    br_ibge_ipca15_mes_categoria_municipio,
    br_ibge_ipca15_mes_categoria_brasil]

flows_cvm=[br_cvm_ofe_pub_dis_dia, br_cvm_adm_car_res, br_cvm_adm_car_pes_fis, br_cvm_adm_car_pes_jur]

flows = reduce(lambda x,y: x+y, [flows_ipca,flows_inpc,flows_ipca15, flows_cvm])

for flow in flows:
    run_local(flow)
