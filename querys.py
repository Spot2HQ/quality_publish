
def query_qa_price_rent(price_m2: float, sector: str):
  query = f"""
    SELECT
      {price_m2} BETWEEN precio_m2_inferior AND precio_m2_superior AS dentro_del_rango
    FROM qa_limites_p_r
    WHERE sector = '{sector}'
  """
  return query

def query_qa_price_sale(price_m2: float, sector: str):
  query = f"""
    SELECT
      {price_m2} BETWEEN precio_m2_inferior AND precio_m2_superior AS dentro_del_rango
    FROM qa_limites_p_s
    WHERE sector = '{sector}'
  """
  return query

def query_qa_area(area: float, sector: str):
  query = f"""
    SELECT
      {area} BETWEEN area_limite_inferior AND area_limite_superior AS dentro_del_rango
    FROM qa_limites_a_s
    WHERE sector = '{sector}'
  """
  return query