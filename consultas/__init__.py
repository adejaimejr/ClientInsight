"""
Pacote de consultas para o sistema ClientInsight.
Este pacote contém todas as consultas específicas para extração e processamento de dados de clientes.
"""

# Importa todas as consultas para facilitar o acesso
from .faturamento import obter_faturamento_ultimos_12_meses
from .ciclos_compra import obter_ciclos_compra_ultimos_6_meses
from .pecas_compradas import obter_total_pecas_compradas
from .titulos_pagos import obter_titulos_pagos_em_dia
from .valor_por_marca import obter_valor_por_marca, obter_numero_marcas_diferentes
from .data_primeira_compra import obter_data_primeira_compra
from .cliente import obter_codigo_cliente, obter_nome_completo
