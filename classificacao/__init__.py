"""
Package para classificação de clientes com base em critérios de faturamento, frequência, pontualidade, volume e diversificação.
"""

from .classificar import classificar_cliente

__all__ = ['classificar_cliente']
