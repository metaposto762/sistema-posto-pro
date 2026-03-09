import streamlit as st
import pandas as pd
import re
import os
import io
import time 
from datetime import datetime, timedelta

# --- IMPORTAÇÕES ---
try:
    import gspread
    from google.oauth2.service_account import Credentials
    import extra_streamlit_components as stx
    HAS_GSPREAD = True
    HAS_COOKIES = True
except ImportError:
    HAS_GSPREAD = False
    HAS_COOKIES = False

try:
    from reportlab.lib.pagesizes import landscape, A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

# ==========================================
# 🛑 CONFIGURAÇÕES E PLANILHA
# ==========================================
try:
    PLANILHA_ID = st.secrets["PLANILHA_ID"]
except:
    PLANILHA_ID = ""

st.set_page_config(page_title="AutoPosto Pro", page_icon="⛽", layout="wide")

# ==========================================
# 🔐 GERENCIADOR DE COOKIES (ESTRATÉGIA DE LOOP)
# ==========================================
if 'autenticado' not in st.session_state:
    st.session_state['autenticado'] = False
    st.session_state['usuario_logado'] = ""
    st.session_state['perfil_logado'] = ""

cookie_manager = stx.CookieManager(key="mestre_posto")

# --- LÓGICA DE RECUPERAÇÃO DE SESSÃO (F5) ---
if not st.session_state['autenticado']:
    # Tenta ler os cookies até 3 vezes com micro-pausas (Para vencer a lentidão do navegador)
    with st.spinner("Verificando sessão..."):
        for _ in range(3):
            all_cookies = cookie_manager.get_all()
            if all_cookies and "user_posto" in all_cookies:
                st.session_state['autenticado'] = True
                st.session_state['usuario_logado'] = all_cookies["user_posto"]
                st.session_state['perfil_logado'] = all_cookies.get("perfil_posto", "Operador")
                st.rerun()
            time.sleep(0.3)

# ==========================================
# MOTOR DE BANCO DE DADOS
# ==========================================
@st.cache_resource
def get_gsheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(creds)

def carregar_dados():
    try:
        client = get_gsheets_client()
        doc = client.open_by_key(PLANILHA_ID)
        
        def load_ws(name, default_cols):
            try:
                ws = doc.worksheet(name)
                data = ws.get_all_records()
                return pd.DataFrame(data) if data else pd.DataFrame(columns=default_cols)
            except: return pd.DataFrame(columns=default_cols)

        st.session_state['empresas'] = load_ws('empresas', ['Posto', 'Status'])
        st.session_state['turnos'] = load_ws('turnos', ['Turno', 'Status'])
        st.session_state['equipe'] = load_ws('equipe', ['Posto', 'Turno', 'Cargo', 'Nome', 'Status'])
        st.session_state['usuarios'] = load_ws('usuarios', ['Usuario', 'Senha', 'Perfil', 'Status'])
        st.session_state['vendas'] = load_ws('vendas', ['Arquivo', 'Nome', 'Mes', 'Atendimentos', 'GC', 'GA', 'S10 - A', 'ETANOL'])
        st.session_state['processados_list'] = load_ws('log', ['id', 'Arquivo', 'Mês', 'Tipo']).to_dict('records')
        st.session_state['config'] = load_ws('config', ['Meta_Dia', 'Meta_Noite'])
        st.session_state['aniversarios'] = load_ws('aniversarios', ['Posto', 'Nome', 'Gênero', 'Data de Nascimento'])
        st.session_state['log_acessos'] = load_ws('log_acessos', ['Data/Hora', 'Usuário', 'Perfil'])
        
        if st.session_state['config'].empty: 
            st.session_state['config'] = pd.DataFrame({'Meta_Dia': [19.63], 'Meta_Noite': [15.00]})
    except Exception as e:
        st.error(f"Erro ao conectar ao Google: {e}")
        st.stop()

def salvar_dados():
    try:
        client = get_gsheets_client()
        doc = client.open_by_key(PLANILHA_ID)
        def save_ws(name, df):
            try: ws = doc.worksheet(name)
            except: ws = doc.add_worksheet(title=name, rows="1000", cols="20")
            ws.clear()
            df_clean = df.fillna("").astype(str)
            dados = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
            ws.update(values=dados, range_name='A1')
            
        save_ws('empresas', st.session_state['empresas'])
        save_ws('turnos', st.session_state['turnos'])
        save_ws('equipe', st.session_state['equipe'])
        save_ws('vendas', st.session_state['vendas'])
        save_ws('config', st.session_state['config'])
        save_ws('aniversarios', st.session_state['aniversarios'])
        save_ws('usuarios', st.session_state['usuarios'])
        save_ws('log_acessos', st.session_state['log_acessos'])
        save_ws('log', pd.DataFrame(st.session_state['processados_list']))
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")

# ==========================================
# TELA DE LOGIN
# ==========================================
if not st.session_state['autenticado']:
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown("<h2 style='text-align: center;'>⛽ AutoPosto Pro</h2>", unsafe_allow_html=True)
            with st.form("login_form"):
                u = st.text_input("Usuário").strip()
                p = st.text_input("Senha", type="password").strip()
                if st.form_submit_button("Entrar", use_container_width=True, type="primary"):
                    carregar_dados() # Busca usuários no Google
                    master_u = st.secrets["credenciais_acesso"]["usuario"]
                    master_p = st.secrets["credenciais_acesso"]["senha"]
                    
                    user_ok = False
                    if u == master_u and p == master_p:
                        st.session_state['usuario_logado'], st.session_state['perfil_logado'] = "Admin Master", "Admin"
                        user_ok = True
                    else:
                        df_u = st.session_state['usuarios']
                        busca = df_u[(df_u['Usuario'] == u) & (df_u['Senha'] == p) & (df_u['Status'] == 'Ativo')]
                        if not busca.empty:
                            st.session_state['usuario_logado'], st.session_state['perfil_logado'] = u, busca.iloc[0]['Perfil']
                            user_ok = True
                    
                    if user_ok:
                        cookie_manager.set("user_posto", st.session_state['usuario_logado'], max_age=30*24*60*60)
                        cookie_manager.set("perfil_posto", st.session_state['perfil_logado'], max_age=30*24*60*60)
                        st.session_state['autenticado'] = True
                        
                        # Log de acesso
                        agora = (datetime.utcnow() - timedelta(hours=3)).strftime("%d/%m/%Y %H:%M:%S")
                        st.session_state['log_acessos'] = pd.concat([st.session_state['log_acessos'], pd.DataFrame([{'Data/Hora': agora, 'Usuário': st.session_state['usuario_logado'], 'Perfil': st.session_state['perfil_logado']}])])
                        salvar_dados()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Usuário ou Senha incorretos.")
    st.stop()

# ==========================================
# SISTEMA LOGADO
# ==========================================
if 'empresas' not in st.session_state: carregar_dados()

with st.sidebar:
    st.title("⛽ AutoPosto Pro")
    st.markdown(f"👤 **{st.session_state['usuario_logado']}**")
    st.markdown("---")
    
    # Menu dinâmico
    paginas = ["📊 Painel Geral", "💰 Bonificação", "🎂 Aniversariantes", "🏢 Cadastro Empresa", "⏰ Cadastro Turnos", "👤 Cadastro Colaborador", "📈 Importar Planilhas"]
    if st.session_state['perfil_logado'] == "Admin": paginas.append("🔐 Gestão de Acessos")
    menu = st.radio("Menu", paginas)
    
    st.markdown("---")
    if st.button("🔄 Atualizar Dados", use_container_width=True):
        carregar_dados()
        st.rerun()
        
    if st.button("🚪 Sair do Sistema", use_container_width=True, type="secondary"):
        cookie_manager.delete("user_posto")
        cookie_manager.delete("perfil_posto")
        st.session_state.clear()
        time.sleep(1)
        st.rerun()

# --- A PARTIR DAQUI, O RESTO DO SEU CÓDIGO (TELAS E CÁLCULOS) ---
# (O código das telas continua exatamente igual ao que você já tem,
# apenas certifique-se de usar st.session_state['vendas'], etc.)

st.write(f"Você está na tela: {menu}")
# [Aqui você pode colar as funções f_br, calcular_dataframe_resultados e os blocos if menu == "..." que você já tem]