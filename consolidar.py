from flask import Flask, request, send_file, render_template_string
import pandas as pd
import numpy as np
import math
import warnings
import io
import os

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

app = Flask(__name__)

# Formulario HTML minimalista para subir los archivos
HTML_FORM = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Módulo de Consolidación BDC</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background-color: #f4f7f6; }
        .container { background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); max-width: 600px; margin: auto; }
        h2 { color: #1F4E78; }
        label { font-weight: bold; display: block; margin-top: 15px; }
        input[type="file"] { margin-top: 5px; }
        button { margin-top: 25px; padding: 10px 20px; background-color: #B71C1C; color: white; border: none; border-radius: 4px; font-size: 16px; cursor: pointer; }
        button:hover { background-color: #901616; }
    </style>
</head>
<body>
    <div class="container">
        <h2>🚀 Módulo Maestro BDC</h2>
        <p>Sube tus 4 archivos de Excel para generar la base consolidada.</p>
        <form action="/procesar" method="post" enctype="multipart/form-data">
            <label>1. Archivo U297:</label> <input type="file" name="u297" accept=".xlsx" required>
            <label>2. Archivo U307:</label> <input type="file" name="u307" accept=".xlsx" required>
            <label>3. Reporte TDM (Rpt):</label> <input type="file" name="rpt" accept=".xlsx" required>
            <label>4. Ventas Quiter:</label> <input type="file" name="ventas" accept=".xlsx" required>
            <button type="submit">Procesar y Descargar Excel</button>
        </form>
    </div>
</body>
</html>
"""

def procesar_datos(file_u297, file_u307, file_rpt, file_ventas):
    # =====================================================================
    # LÓGICA DE PROCESAMIENTO (TU CÓDIGO ORIGINAL)
    # =====================================================================
    df_u297 = pd.read_excel(file_u297)
    df_u307 = pd.read_excel(file_u307)

    if 'Bastidor' in df_u307.columns:
        df_u307 = df_u307.rename(columns={'Bastidor': 'VIN'})
    if 'Descripción' in df_u307.columns:
        df_u307 = df_u307.rename(columns={'Descripción': 'Desc_Servicio'})

    df_u297['Refer.'] = df_u297['Refer.'].astype(str).str.strip()
    df_u297['VIN'] = df_u297['VIN'].astype(str).str.strip().str.upper()
    df_u307['Refer.'] = df_u307['Refer.'].astype(str).str.strip()
    df_u307['VIN'] = df_u307['VIN'].astype(str).str.strip().str.upper()

    cols_u307 = ['Refer.', 'VIN', 'Modelo', 'Km', 'T.Mano obr', 'T.Refaccio']
    df_u307_rec = df_u307[[c for c in cols_u307 if c in df_u307.columns]].drop_duplicates()
    df_consolidado = pd.merge(df_u297, df_u307_rec, on=['Refer.', 'VIN'], how='left')

    df_consolidado['F.cierre'] = pd.to_datetime(df_consolidado['F.cierre'], errors='coerce')
    meses_esp = {1: 'enero', 2: 'febrero', 3: 'marzo', 4: 'abril', 5: 'mayo', 6: 'junio', 
                 7: 'julio', 8: 'agosto', 9: 'septiembre', 10: 'octubre', 11: 'noviembre', 12: 'diciembre'}
    df_consolidado['mes'] = df_consolidado['F.cierre'].dt.month.map(meses_esp)
    df_consolidado['año'] = df_consolidado['F.cierre'].dt.year
    df_consolidado['Servicio'] = df_consolidado.get('Descripción', pd.NA)
    df_consolidado['Modelo_base'] = df_consolidado.get('Modelo', pd.Series(dtype=str)).astype(str).str.split(' ').str[0]
    
    def cat_grupo(desc):
        d = str(desc).upper()
        return 'Mantenimiento' if 'MANTENIMIENTO' in d else ('Reparación' if 'REPARACION' in d or 'REEMPLAZO' in d else 'Mixto')
    
    df_consolidado['TipoGrupo'] = df_consolidado.get('DESCRIPCION TIPO MO', pd.Series(dtype=str)).apply(cat_grupo)
    df_consolidado['Aseso'] = pd.NA
    
    cols_lineas = ['Refer.', 'F.cierre', 'VIN', 'DESCRIPCION TIPO MO', 'Servicio', 'Modelo', 'Km', 'T.Mano obr', 'T.Refaccio', 'Aseso', 'Asesor', 'mes', 'año', 'TipoGrupo', 'Modelo_base']
    for c in cols_lineas:
        if c not in df_consolidado.columns: df_consolidado[c] = pd.NA
    
    df_lineas = df_consolidado[cols_lineas].copy()
    
    df_ordenes = df_lineas.groupby('Refer.').agg({
        'F.cierre': 'first', 'VIN': 'first', 'Modelo_base': 'first', 'Modelo': 'first', 'Km': 'first',
        'T.Mano obr': 'first', 'T.Refaccio': 'first', 'Asesor': 'first', 'año': 'first', 'mes': 'first', 'TipoGrupo': 'first'
    }).reset_index()
    
    df_ordenes = pd.merge(df_ordenes, df_lineas.groupby('Refer.').size().reset_index(name='Lineas'), on='Refer.')
    df_ordenes['Total_OS'] = pd.to_numeric(df_ordenes['T.Mano obr'], errors='coerce').fillna(0) + pd.to_numeric(df_ordenes['T.Refaccio'], errors='coerce').fillna(0)
    df_ordenes['Categoria_OS'] = df_ordenes['TipoGrupo']
    
    def rango_km(km):
        try:
            k = float(km)
            return '0-10k' if k < 10000 else '10-20k' if k < 20000 else '20-30k' if k < 30000 else '30-50k' if k < 50000 else '50-100k' if k < 100000 else '100k+'
        except: return 'N/A'
    df_ordenes['Rango_km'] = df_ordenes['Km'].apply(rango_km)
    
    cols_ord = ['Refer.', 'F.cierre', 'VIN', 'Modelo_base', 'Modelo', 'Km', 'T.Mano obr', 'T.Refaccio', 'Asesor', 'año', 'mes', 'TipoGrupo', 'Lineas', 'Total_OS', 'Categoria_OS', 'Rango_km']
    for c in cols_ord:
        if c not in df_ordenes.columns: df_ordenes[c] = pd.NA
    df_ordenes = df_ordenes[cols_ord]

    try:
        df_vtas = pd.read_excel(file_ventas, sheet_name="FACTURACION")
        df_vtas['VIN'] = df_vtas['Bastidor'].astype(str).str.strip().str.upper()
        df_vtas['F.cierre'] = pd.to_datetime(df_vtas['F.cierre'], errors='coerce')
        df_vtas = df_vtas.sort_values('F.cierre').drop_duplicates('VIN', keep='last')
        df_vtas_clean = df_vtas[['VIN', 'F.cierre']].rename(columns={'F.cierre': 'Fecha_Venta_Quiter'})
        df_vtas_clean['Vendido_En_Quiter'] = True
    except Exception as e:
        df_vtas_clean = pd.DataFrame(columns=['VIN', 'Fecha_Venta_Quiter', 'Vendido_En_Quiter'])

    df_lista = pd.read_excel(file_rpt, sheet_name="Lista objetivo para Citas", skiprows=9)
    col_vin_tdm = 'VIN' if 'VIN' in df_lista.columns else 'Vin'
    df_lista['VIN_TDM'] = df_lista[col_vin_tdm].astype(str).str.strip().str.upper()
    df_lista['Fecha_Servicio_TDM'] = pd.to_datetime(df_lista.get('Fecha Último Servicio'), errors='coerce')
    df_lista['Modelo_TDM'] = df_lista.get('Modelo', 'DESCONOCIDO').astype(str).str.strip().str.upper()

    df_lineas = df_lineas.sort_values(by=['VIN', 'F.cierre'], ascending=[True, True])
    df_servicios_extra = df_lineas.groupby('VIN').agg(
        Tipos_Servicio=('DESCRIPCION TIPO MO', lambda x: ', '.join(x.dropna().astype(str).unique())),
        Ultimo_Tipo_Servicio=('DESCRIPCION TIPO MO', 'last')
    ).reset_index()

    df_srv_grouped = df_ordenes.groupby('VIN').agg(
        Modelo=('Modelo_base', 'last'),
        Total_Visitas=('Refer.', 'count'),
        Primera_Visita=('F.cierre', 'min'),
        Ultima_Visita=('F.cierre', 'max'),
        Km_Primer_Servicio=('Km', 'min'),
        Km_Actual=('Km', 'max'),
        LTV_Monto_Total=('Total_OS', 'sum')
    ).reset_index()

    df_srv_grouped = pd.merge(df_srv_grouped, df_servicios_extra, on='VIN', how='left')

    cols_tdm = ['VIN_TDM', 'Tipo de Seguimiento', 'Primer Serv. o Recordatorio', 'Fecha_Servicio_TDM', 'Modelo_TDM']
    df_lista_limpia = df_lista[[c for c in cols_tdm if c in df_lista.columns]].drop_duplicates('VIN_TDM')
    
    hist_vins = pd.merge(df_srv_grouped, df_lista_limpia, left_on='VIN', right_on='VIN_TDM', how='outer')
    hist_vins['VIN'] = hist_vins['VIN'].fillna(hist_vins['VIN_TDM'])
    hist_vins['Modelo'] = hist_vins['Modelo'].fillna(hist_vins['Modelo_TDM']).str.upper()
    hist_vins['Ultima_Visita'] = hist_vins['Ultima_Visita'].fillna(hist_vins['Fecha_Servicio_TDM'])
    
    hist_vins = pd.merge(hist_vins, df_vtas_clean, on='VIN', how='left')

    try:
        df_ventas_tdm = pd.read_excel(file_rpt, sheet_name="BD Distribuidor", skiprows=8)
        col_vin_bd = 'Vin' if 'Vin' in df_ventas_tdm.columns else ('VIN' if 'VIN' in df_ventas_tdm.columns else None)
        
        if col_vin_bd and 'Distribuidor Venta' in df_ventas_tdm.columns:
            df_ventas_tdm_clean = df_ventas_tdm[[col_vin_bd, 'Distribuidor Venta']].copy()
            df_ventas_tdm_clean = df_ventas_tdm_clean.rename(columns={col_vin_bd: 'VIN', 'Distribuidor Venta': 'Origen_Venta_TDM'})
            df_ventas_tdm_clean['VIN'] = df_ventas_tdm_clean['VIN'].astype(str).str.strip().str.upper()
            df_ventas_tdm_clean = df_ventas_tdm_clean.drop_duplicates(subset=['VIN'], keep='last')
            
            hist_vins = pd.merge(hist_vins, df_ventas_tdm_clean, on='VIN', how='left')
        else:
            hist_vins['Origen_Venta_TDM'] = np.nan
    except:
        hist_vins['Origen_Venta_TDM'] = np.nan

    condiciones_venta = [
        hist_vins['Vendido_En_Quiter'] == True,                                           
        hist_vins['Origen_Venta_TDM'].str.contains('DISTRIBUIDOR', case=False, na=False)  
    ]
    opciones_venta = ['VENDIDO AQUÍ', 'VENDIDO AQUÍ']
    hist_vins['Estado_Venta'] = np.select(condiciones_venta, opciones_venta, default='OTRA AGENCIA')
    hist_vins['Fecha_Venta'] = hist_vins['Fecha_Venta_Quiter']

    hist_vins['Total_Visitas'] = hist_vins['Total_Visitas'].fillna(0)
    hist_vins['LTV_Monto_Total'] = hist_vins['LTV_Monto_Total'].fillna(0)
    hist_vins['Km_Actual'] = pd.to_numeric(hist_vins['Km_Actual'], errors='coerce').fillna(0)
    hist_vins['Km_Primer_Servicio'] = pd.to_numeric(hist_vins['Km_Primer_Servicio'], errors='coerce').fillna(0)
    hist_vins['Ultimo_Tipo_Servicio'] = hist_vins['Ultimo_Tipo_Servicio'].fillna('SIN REGISTRO EN TALLER')
    hist_vins['Tipos_Servicio'] = hist_vins['Tipos_Servicio'].fillna('N/D')

    def alerta_tdm(r):
        if pd.isna(r['VIN_TDM']): return "NO APLICA (FUERA DE META TDM)"
        t = str(r.get('Primer Serv. o Recordatorio', 'OBJETIVO GENERAL')).strip().upper()
        s = str(r.get('Tipo de Seguimiento', '')).strip().upper()
        return f"¡OBJETIVO TDM! {t} ({s})"
    hist_vins['Alerta_KPI_TDM'] = hist_vins.apply(alerta_tdm, axis=1)

    fecha_actual = df_ordenes['F.cierre'].max() if not pd.isna(df_ordenes['F.cierre'].max()) else pd.to_datetime('today')
    fecha_ref = hist_vins['Ultima_Visita'].fillna(hist_vins['Fecha_Venta'])
    dias_calc = (fecha_actual - fecha_ref).dt.days
    hist_vins['Dias_Sin_Venir'] = dias_calc.fillna(999).astype(int)
    hist_vins['Meses_Sin_Venir'] = (hist_vins['Dias_Sin_Venir'] / 30.44).round(1)
    hist_vins['Status_Retencion'] = np.where(hist_vins['Dias_Sin_Venir'] <= 365, 'ACTIVO (RETENIDO)', 'INACTIVO (PERDIDO > 1 AÑO)')
    
    es_vendido_aqui = hist_vins['Estado_Venta'] == 'VENDIDO AQUÍ'
    condiciones_crt = [
        (hist_vins['Dias_Sin_Venir'] <= 365) & es_vendido_aqui,
        (hist_vins['Dias_Sin_Venir'] <= 365) & ~es_vendido_aqui,
        (hist_vins['Dias_Sin_Venir'] > 365) & es_vendido_aqui,
        (hist_vins['Dias_Sin_Venir'] > 365) & ~es_vendido_aqui
    ]
    opciones_crt = [
        '1. RETENCIÓN PURA (VENDIDO AQUÍ Y ACTIVO)',
        '2. CONQUISTA (VENDIDO EN OTRA AGENCIA Y ACTIVO)',
        '3. FUGA PURA (VENDIDO AQUÍ Y PERDIDO)',
        '4. FUGA CONQUISTA (CLIENTE EXTERNO PERDIDO)'
    ]
    hist_vins['Clasificacion_CRT'] = np.select(condiciones_crt, opciones_crt, default='DESCONOCIDO')

    hist_vins['Dias_Historicos'] = (hist_vins['Ultima_Visita'] - hist_vins['Primera_Visita']).dt.days.fillna(0)
    hist_vins['DERT_Dias_Promedio'] = np.where((hist_vins['Total_Visitas'] > 1) & (hist_vins['Dias_Historicos'] > 0), hist_vins['Dias_Historicos'] / (hist_vins['Total_Visitas'] - 1), 180).astype(int)
    hist_vins['DERT_Dias_Promedio'] = hist_vins['DERT_Dias_Promedio'].replace(0, 180)
    hist_vins['Meses_Entre_Servicios_Promedio'] = (hist_vins['DERT_Dias_Promedio'] / 30.44).round(1)
    
    hist_vins['Indice_Riesgo_Churn_%'] = ((hist_vins['Dias_Sin_Venir'] / hist_vins['DERT_Dias_Promedio']) * 100).round(2)
    def churn(pct): return "1. CHURN CONFIRMADO" if pct >= 150 else ("2. RIESGO CRÍTICO" if pct >= 100 else ("3. RIESGO ALTO" if pct >= 80 else "4. SANO"))
    hist_vins['Alerta_Churn'] = hist_vins['Indice_Riesgo_Churn_%'].apply(churn)

    hist_vins['Km_Diario'] = np.where(hist_vins['Dias_Historicos'] > 0, (hist_vins['Km_Actual'] - hist_vins['Km_Primer_Servicio']) / hist_vins['Dias_Historicos'], (hist_vins['Km_Actual'] / 180))
    hist_vins['CERT_Km_Proyectado_Hoy'] = (hist_vins['Km_Actual'] + (hist_vins['Km_Diario'] * hist_vins['Dias_Sin_Venir'])).fillna(hist_vins['Km_Actual']).astype(int)

    def prox_serv(km): return "OFRECER 1ER SERVICIO" if km <= 0 else f"OFRECER PAQUETE DE {math.ceil(km / 10000) * 10000:,} KM"
    hist_vins['Proximo_Serv_Recomendado'] = hist_vins['CERT_Km_Proyectado_Hoy'].apply(prox_serv)

    def prioridad(r):
        if "OBJETIVO TDM" in r['Alerta_KPI_TDM']: return "0. CRÍTICO TDM"
        if r['Status_Retencion'] == 'INACTIVO (PERDIDO > 1 AÑO)': return "5. CAMPAÑA RECUPERACIÓN"
        if 'HILUX' in str(r['Modelo']).upper() and 35000 <= r['CERT_Km_Proyectado_Hoy'] <= 45000: return "1. OPORTUNIDAD VIP: HILUX 40K"
        if r['Dias_Sin_Venir'] >= r['DERT_Dias_Promedio']: return "2. URGENCIA: CICLO VENCIDO"
        if r['Dias_Sin_Venir'] >= (r['DERT_Dias_Promedio'] - 30): return "3. PREVENTIVO: PRÓXIMO A VENCER"
        return "4. SEGUIMIENTO REGULAR"
    hist_vins['Estrategia_Llamada'] = hist_vins.apply(prioridad, axis=1)

    hist_vins = hist_vins.sort_values(by=['Estrategia_Llamada', 'LTV_Monto_Total'], ascending=[True, False])

    cols_fin = [
        'VIN', 'Modelo', 'Alerta_KPI_TDM', 'Estrategia_Llamada', 'Proximo_Serv_Recomendado',
        'Estado_Venta', 'Clasificacion_CRT', 'Status_Retencion', 'Alerta_Churn', 'Indice_Riesgo_Churn_%',
        'LTV_Monto_Total', 'Dias_Sin_Venir', 'Meses_Sin_Venir', 'DERT_Dias_Promedio', 'Meses_Entre_Servicios_Promedio',
        'Km_Primer_Servicio', 'Km_Actual', 'CERT_Km_Proyectado_Hoy',
        'Fecha_Venta', 'Primera_Visita', 'Ultima_Visita', 'Ultimo_Tipo_Servicio', 'Total_Visitas', 'Tipos_Servicio'
    ]
    df_bdc = hist_vins[cols_fin].copy().fillna('N/D')

    df_lineas['F.cierre'] = df_lineas['F.cierre'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_ordenes['F.cierre'] = df_ordenes['F.cierre'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_bdc['Fecha_Venta'] = pd.to_datetime(df_bdc['Fecha_Venta'], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d').fillna('SIN REGISTRO')
    df_bdc['Primera_Visita'] = pd.to_datetime(df_bdc['Primera_Visita'], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d').fillna('SIN REGISTRO')
    df_bdc['Ultima_Visita'] = pd.to_datetime(df_bdc['Ultima_Visita'], errors='coerce', format='mixed').dt.strftime('%Y-%m-%d').fillna('SIN REGISTRO')

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_lineas.to_excel(writer, sheet_name='Lineas', index=False)
        df_ordenes.to_excel(writer, sheet_name='Ordenes', index=False)
        df_bdc.to_excel(writer, sheet_name='Base_BDC', index=False)
        
        # Formato básico (sin colores para asegurar velocidad en web)
        ws_bdc = writer.sheets['Base_BDC']
        ws_bdc.column_dimensions['A'].width = 20  
        ws_bdc.column_dimensions['C'].width = 45  
        ws_bdc.column_dimensions['D'].width = 45  
        ws_bdc.column_dimensions['E'].width = 38  
        ws_bdc.column_dimensions['G'].width = 48  
        ws_bdc.column_dimensions['X'].width = 60  

    return output.getvalue()


@app.route('/')
def index():
    return render_template_string(HTML_FORM)

@app.route('/procesar', methods=['POST'])
def procesar():
    try:
        f_u297 = request.files['u297']
        f_u307 = request.files['u307']
        f_rpt = request.files['rpt']
        f_ventas = request.files['ventas']

        excel_bytes = procesar_datos(f_u297, f_u307, f_rpt, f_ventas)

        return send_file(
            io.BytesIO(excel_bytes),
            download_name='ESTRATEGIA_BDC_FINAL.xlsx',
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        return f"<h3>❌ Error procesando los archivos:</h3><p>{str(e)}</p>", 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
