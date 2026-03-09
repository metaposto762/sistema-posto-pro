import streamlit as st
import pandas as pd
import re
import os
import io
import time # Motor de tempo adicionado!
from datetime import datetime, timedelta

# --- IMPORTAÇÕES DO GOOGLE SHEETS ---
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

# --- IMPORTAÇÕES DO GERADOR DE PDF ---
try:
    from reportlab.lib.pagesizes import landscape, A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False

# --- IMPORTAÇÕES DE COOKIES (MANTER LOGADO) ---
try:
    import extra_streamlit_components as stx
    HAS_COOKIES = True
except ImportError:
    HAS_COOKIES = False

# ==========================================
# 🛑 PUXANDO O ID DA PLANILHA DO COFRE SECRETO:
# ==========================================
try:
    PLANILHA_ID = st.secrets["PLANILHA_ID"]
except KeyError:
    PLANILHA_ID = "" # Prevenção de erro caso esqueça de colocar no Streamlit

# --- Configuração da Página ---
st.set_page_config(page_title="AutoPosto Pro", page_icon="⛽", layout="wide", initial_sidebar_state="expanded")

# ==========================================
# INJEÇÃO DE CSS
# ==========================================
st.markdown("""
<style>
    [data-testid="metric-container"] { padding: 1.2rem 1.5rem !important; border-radius: 12px !important; border: 1px solid rgba(128, 128, 128, 0.2) !important; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06) !important; transition: transform 0.2s ease, box-shadow 0.2s ease !important; }
    [data-testid="metric-container"]:hover { transform: translateY(-4px) !important; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.2) !important; }
    [data-testid="stMetricLabel"] p { font-weight: 600 !important; font-size: 1.05rem !important; }
    [data-testid="stMetricValue"] { font-weight: 800 !important; }
    div[data-testid="stVerticalBlock"] > div > div > div > div:nth-child(1) [data-testid="metric-container"] { border-left: 5px solid #3b82f6 !important; } 
    div[data-testid="stVerticalBlock"] > div > div > div > div:nth-child(2) [data-testid="metric-container"] { border-left: 5px solid #10b981 !important; } 
    div[data-testid="stVerticalBlock"] > div > div > div > div:nth-child(3) [data-testid="metric-container"] { border-left: 5px solid #f59e0b !important; } 
    div[data-testid="stVerticalBlock"] > div > div > div > div:nth-child(4) [data-testid="metric-container"] { border-left: 5px solid #ef4444 !important; } 
    div[data-testid="stVerticalBlock"] > div[style*="border"] { border-radius: 12px !important; box-shadow: 0 1px 3px 0 rgba(0,0,0,0.1) !important; padding: 1.5rem !important; margin-bottom: 1rem !important; }
    @media (max-width: 768px) {
        div[data-testid="stVerticalBlock"] > div[style*="border"] { padding: 1rem 0.8rem !important; }
        [data-testid="metric-container"] { padding: 0.8rem 1rem !important; }
        [data-testid="stMetricValue"] { font-size: 1.4rem !important; }
        [data-testid="stMetricLabel"] p { font-size: 0.9rem !important; }
        h1 { font-size: 1.6rem !important; } h2 { font-size: 1.3rem !important; } h3 { font-size: 1.1rem !important; }
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# MOTOR DE BANCO DE DADOS (GOOGLE SHEETS)
# ==========================================
@st.cache_resource
def get_gsheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if os.path.exists('credenciais.json'):
        creds = Credentials.from_service_account_file('credenciais.json', scopes=scope)
    else:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(creds)

def carregar_dados():
    if not HAS_GSPREAD:
        st.error("⚠️ Biblioteca do Google Sheets não instalada! Rode: pip install gspread google-auth")
        st.stop()
        
    try:
        client = get_gsheets_client()
        doc = client.open_by_key(PLANILHA_ID)
        
        def load_ws(name, default_cols):
            try:
                ws = doc.worksheet(name)
                data = ws.get_all_records()
                if data: return pd.DataFrame(data)
                else: return pd.DataFrame(columns=default_cols)
            except Exception:
                return pd.DataFrame(columns=default_cols)

        st.session_state['empresas'] = load_ws('empresas', ['Posto', 'Status'])
        st.session_state['turnos'] = load_ws('turnos', ['Turno', 'Status'])
        st.session_state['equipe'] = load_ws('equipe', ['Posto', 'Turno', 'Cargo', 'Nome', 'Status'])
        
        df_usr = load_ws('usuarios', ['Usuario', 'Senha', 'Perfil', 'Status'])
        df_usr['Usuario'] = df_usr['Usuario'].astype(str).str.strip()
        df_usr['Senha'] = df_usr['Senha'].astype(str).str.strip()
        st.session_state['usuarios'] = df_usr
        
        df_vendas = load_ws('vendas', ['Arquivo', 'Nome', 'Mes', 'Atendimentos', 'GC', 'GA', 'S10 - A', 'ETANOL'])
        for col in ['Atendimentos', 'GC', 'GA', 'S10 - A', 'ETANOL']:
            if col in df_vendas.columns:
                df_vendas[col] = pd.to_numeric(df_vendas[col], errors='coerce').fillna(0.0)
        st.session_state['vendas'] = df_vendas
        
        log_df = load_ws('log', ['id', 'Arquivo', 'Mês', 'Tipo'])
        st.session_state['processados_list'] = log_df.to_dict('records')
        
        config_df = load_ws('config', ['Meta_Dia', 'Meta_Noite'])
        if config_df.empty: st.session_state['config'] = pd.DataFrame({'Meta_Dia': [19.63], 'Meta_Noite': [15.00]})
        else: st.session_state['config'] = config_df
        
        st.session_state['aniversarios'] = load_ws('aniversarios', ['Posto', 'Nome', 'Gênero', 'Data de Nascimento'])
        
        st.session_state['log_acessos'] = load_ws('log_acessos', ['Data/Hora', 'Usuário', 'Perfil'])
        
    except Exception as e:
        st.error(f"⚠️ Erro ao conectar com a Planilha do Google: {e}")
        st.stop()

def salvar_dados():
    if not HAS_GSPREAD: return
    try:
        client = get_gsheets_client()
        doc = client.open_by_key(PLANILHA_ID)
        
        def save_ws(name, df):
            try: ws = doc.worksheet(name)
            except gspread.exceptions.WorksheetNotFound: ws = doc.add_worksheet(title=name, rows="1000", cols="20")
            ws.clear()
            df_clean = df.fillna("").astype(str)
            dados = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
            try: ws.update(values=dados, range_name='A1')
            except TypeError: ws.update('A1', dados)
            
        save_ws('empresas', st.session_state['empresas'])
        save_ws('turnos', st.session_state['turnos'])
        save_ws('equipe', st.session_state['equipe'])
        save_ws('vendas', st.session_state['vendas'])
        save_ws('config', st.session_state['config'])
        save_ws('aniversarios', st.session_state['aniversarios'])
        save_ws('usuarios', st.session_state['usuarios'])
        save_ws('log_acessos', st.session_state['log_acessos'])
        
        log_df = pd.DataFrame(st.session_state['processados_list']) if st.session_state['processados_list'] else pd.DataFrame(columns=['id', 'Arquivo', 'Mês', 'Tipo'])
        save_ws('log', log_df)
    except Exception as e:
        st.error(f"🛑 Erro ao salvar os dados na nuvem: {e}")
        st.stop()

# ==========================================
# 🔐 GERENCIADOR DE COOKIES E LOGIN
# ==========================================
cookie_manager = None
if HAS_COOKIES:
    cookie_manager = stx.CookieManager(key="cookies_posto")

if 'autenticado' not in st.session_state:
    st.session_state['autenticado'] = False
    st.session_state['usuario_logado'] = ""
    st.session_state['perfil_logado'] = ""

# 🔄 FREIO ANTI-F5 E RECUPERAÇÃO DE SESSÃO
if not st.session_state['autenticado'] and cookie_manager is not None:
    cookies_salvos = cookie_manager.get_all()
    
    # Se o navegador respondeu e achou a chave
    if isinstance(cookies_salvos, dict) and "user_posto" in cookies_salvos and cookies_salvos["user_posto"]:
        st.session_state['autenticado'] = True
        st.session_state['usuario_logado'] = str(cookies_salvos["user_posto"])
        st.session_state['perfil_logado'] = str(cookies_salvos.get("perfil_posto", "Operador"))
        st.rerun()
    else:
        # Se não achou, pode ser porque o navegador foi lento (clássico ao apertar F5).
        # Vamos piscar a tela e tentar de novo em meio segundo antes de deslogar.
        if 'esperou_f5' not in st.session_state:
            st.session_state['esperou_f5'] = True
            time.sleep(0.6) # Freio mágico 🪄
            st.rerun()

if not st.session_state['autenticado']:
    if not HAS_COOKIES:
        st.warning("⚠️ Instale a biblioteca 'extra-streamlit-components' para manter-se logado ao atualizar a página.")
        
    st.markdown("<br><br>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        with st.container(border=True):
            st.markdown("<h2 style='text-align: center;'>⛽ AutoPosto Pro</h2>", unsafe_allow_html=True)
            st.markdown("<p style='text-align: center; color: gray;'>Acesso Restrito</p>", unsafe_allow_html=True)
            st.markdown("---")
            
            with st.form("form_login"):
                usuario_digitado = st.text_input("Usuário").strip()
                senha_digitada = st.text_input("Senha", type="password").strip()
                btn_entrar = st.form_submit_button("Entrar no Sistema", use_container_width=True, type="primary")
                
                if btn_entrar:
                    with st.spinner("Autenticando..."):
                        if 'empresas' not in st.session_state: carregar_dados()
                        
                        try:
                            master_user = str(st.secrets["credenciais_acesso"]["usuario"]).strip()
                            master_pass = str(st.secrets["credenciais_acesso"]["senha"]).strip()
                        except KeyError:
                            master_user, master_pass = None, None
                        
                        df_usr = st.session_state.get('usuarios', pd.DataFrame())
                        user_valido = False
                        
                        if master_user and usuario_digitado == master_user and senha_digitada == master_pass:
                            st.session_state['autenticado'] = True
                            st.session_state['usuario_logado'] = "Admin Master"
                            st.session_state['perfil_logado'] = "Admin"
                            user_valido = True
                        elif not df_usr.empty:
                            busca = df_usr[(df_usr['Usuario'] == usuario_digitado) & (df_usr['Senha'] == senha_digitada) & (df_usr['Status'] == 'Ativo')]
                            if not busca.empty:
                                st.session_state['autenticado'] = True
                                st.session_state['usuario_logado'] = busca.iloc[0]['Usuario']
                                st.session_state['perfil_logado'] = busca.iloc[0]['Perfil']
                                user_valido = True
                        
                        if user_valido:
                            # Registra o acesso no banco de dados
                            agora = datetime.utcnow() - timedelta(hours=3) 
                            novo_log = pd.DataFrame([{'Data/Hora': agora.strftime("%d/%m/%Y %H:%M:%S"), 'Usuário': st.session_state['usuario_logado'], 'Perfil': st.session_state['perfil_logado']}])
                            st.session_state['log_acessos'] = pd.concat([st.session_state.get('log_acessos', pd.DataFrame(columns=['Data/Hora', 'Usuário', 'Perfil'])), novo_log], ignore_index=True)
                            salvar_dados()
                            
                            # Salva o Carimbo no Navegador por 30 dias
                            if cookie_manager is not None:
                                cookie_manager.set("user_posto", st.session_state['usuario_logado'], max_age=30*24*60*60, key="set_u")
                                cookie_manager.set("perfil_posto", st.session_state['perfil_logado'], max_age=30*24*60*60, key="set_p")
                                time.sleep(1.0) # Freio para garantir que o navegador guardou a chave ANTES de entrar!
                                
                            st.rerun()
                        else: st.error("❌ Usuário ou senha incorretos ou inativos!")
    st.stop() 

# Carrega os dados da nuvem para quem já está logado
if 'empresas' not in st.session_state:
    with st.spinner("Conectando ao Servidor Google..."):
        carregar_dados()

# ==========================================
# MOTOR GERADOR DE EXCEL E PDF
# ==========================================
def gerar_excel(df, agrupar_por=None):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        if agrupar_por and agrupar_por in df.columns:
            grupos = sorted([str(x) for x in df[agrupar_por].unique()])
            for g in grupos:
                df_g = df[df[agrupar_por].astype(str) == g].copy()
                df_g.drop(columns=[agrupar_por], inplace=True)
                aba_nome = re.sub(r'[\\/*?:\[\]]', '', str(g))[:31]
                if not aba_nome: aba_nome = "Relatorio"
                df_g.to_excel(writer, index=False, sheet_name=aba_nome)
        else:
            df.to_excel(writer, index=False, sheet_name='Relatório')
    return buffer.getvalue()

def gerar_pdf(df, titulo, agrupar_por=None, texto_total="registros"):
    if not HAS_REPORTLAB: return None
    try:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), rightMargin=15, leftMargin=15, topMargin=15, bottomMargin=15)
        elements = []
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(name='CustomTitle', parent=styles['Title'], fontSize=14, spaceAfter=8)
        subtitle_style = ParagraphStyle(name='CustomSub', parent=styles['Normal'], fontSize=10, fontName='Helvetica-Bold', spaceAfter=6, spaceBefore=12)
        cell_style = ParagraphStyle(name='CustomCell', parent=styles['Normal'], fontSize=6, alignment=1, leading=7)
        header_style = ParagraphStyle(name='CustomHeader', parent=styles['Normal'], fontSize=7, fontName='Helvetica-Bold', textColor=colors.whitesmoke, alignment=1, leading=8)
        
        def safe_text(t):
            t = str(t)
            for emoji in ['💰','⛽','💵','🔴','🟡','⚫','🟢','👥','🏢','🎉','📥','🎂','⏳','🕒','⚖️','📝','💡', '⚡']: t = t.replace(emoji, '')
            return t.strip().replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        
        elements.append(Paragraph(safe_text(titulo), title_style))
        if texto_total:
            elements.append(Paragraph(f"<b>Total Geral:</b> {len(df)} {texto_total}", styles['Normal']))
            elements.append(Spacer(1, 6))
            
        def criar_tabela(df_tabela):
            data = [[Paragraph(safe_text(col), header_style) for col in df_tabela.columns]]
            for _, row in df_tabela.iterrows():
                data.append([Paragraph(safe_text(val), cell_style) for val in row.values])
                
            page_width = landscape(A4)[0] - 30
            pesos = [max(len(safe_text(c)), df_tabela[c].astype(str).apply(lambda x: len(safe_text(x))).max() if len(df_tabela)>0 else 5) for c in df_tabela.columns]
            col_widths = [(p / sum(pesos)) * page_width for p in pesos]
            
            table = Table(data, colWidths=col_widths, repeatRows=1)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#1e293b")),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
            ]))
            return table

        if agrupar_por and agrupar_por in df.columns:
            for g in sorted([str(x) for x in df[agrupar_por].unique()]):
                df_g = df[df[agrupar_por].astype(str) == g].drop(columns=[agrupar_por])
                elements.append(Paragraph(f"Empresa: {safe_text(g)}  |  Total: {len(df_g)} {texto_total}", subtitle_style))
                elements.append(criar_tabela(df_g))
        else:
            elements.append(criar_tabela(df))
            
        doc.build(elements)
        return buffer.getvalue()
    except Exception as e:
        buffer_erro = io.BytesIO()
        doc_erro = SimpleDocTemplate(buffer_erro, pagesize=landscape(A4))
        doc_erro.build([Paragraph(f"Erro ao formatar o PDF.<br/><br/>Técnico: {str(e)}", getSampleStyleSheet()['Normal'])])
        return buffer_erro.getvalue()

# ==========================================
# MENU LATERAL E BOTÃO DE SAIR
# ==========================================
with st.sidebar:
    st.title("⛽ AutoPosto Pro")
    st.markdown(f"👤 Usuário: **{st.session_state['usuario_logado']}**")
    st.markdown("---")
    
    opcoes_menu = ["📊 Painel Geral", "💰 Bonificação", "🎂 Aniversariantes", "🏢 Cadastro Empresa", "⏰ Cadastro Turnos", "👤 Cadastro Colaborador", "📈 Importar Planilhas"]
    
    if st.session_state['perfil_logado'] == 'Admin':
        opcoes_menu.append("🔐 Gestão de Acessos")
        
    menu = st.radio("Navegação do Sistema", opcoes_menu)
    st.markdown("---")
    
    if st.button("🔄 Atualizar Dados do Google", use_container_width=True):
        with st.spinner("Buscando dados no Google..."):
            carregar_dados()
        st.success("Dados Atualizados!")
        st.rerun()

    # BOTÃO DEFINITIVO DE SAIR
    if st.button("🚪 Sair do Sistema", use_container_width=True):
        with st.spinner("Saindo com segurança..."):
            if cookie_manager is not None:
                # Tenta deletar. Se o cookie já sumiu, ele ignora o erro e segue o baile!
                try:
                    cookie_manager.delete("user_posto", key="del_u")
                except KeyError:
                    pass
                
                try:
                    cookie_manager.delete("perfil_posto", key="del_p")
                except KeyError:
                    pass
                    
                time.sleep(1.0) # Freio para garantir que o navegador jogou a chave fora
            
            # Limpa toda a memória da sessão como uma bomba nuclear!
            st.session_state.clear()
            st.rerun()
        
    st.markdown("---")
    st.caption("Versão 6.6 | Sessão Blindada")

# ==========================================
# FUNÇÃO DE CÁLCULO GERAL
# ==========================================
def calcular_dataframe_resultados(mes_sel, posto_sel):
    vendas_mes = st.session_state['vendas'][st.session_state['vendas']['Mes'] == mes_sel]
    vendas_agrupadas = vendas_mes.groupby(['Nome', 'Mes'])[['Atendimentos', 'GC', 'GA', 'S10 - A', 'ETANOL']].sum().reset_index()
    
    df = pd.merge(st.session_state['equipe'], vendas_agrupadas, on='Nome', how='left')
    tem_vendas_neste_mes = df['Nome'].isin(vendas_agrupadas['Nome'])
    df = df[(df['Status'] == 'Ativo') | (tem_vendas_neste_mes)].copy()
    df.fillna(0, inplace=True)

    if posto_sel != "Todos": df = df[df['Posto'] == posto_sel]

    if not df.empty:
        df['Litragem'] = df['GC'] + df['GA'] + df['S10 - A'] + df['ETANOL']
        
        def extrair_horas(t):
            matches = re.findall(r'(\d{1,2})h(?:(\d{1,2})m)?', str(t).lower())
            if len(matches) == 2:
                h1, m1 = matches[0]; h2, m2 = matches[1]
                t1 = int(h1)*60 + (int(m1) if m1 else 0)
                t2 = int(h2)*60 + (int(m2) if m2 else 0)
                if t2 <= t1: t2 += 24*60
                return round((t2 - t1) / 60.0, 2)
            return 0.0

        df['Carga_Horaria'] = df['Turno'].apply(extrair_horas)
        df['Caixa_Visual'] = df.apply(lambda r: f"⏳ Turnos Agrupados ({r['Carga_Horaria']}h)" if r['Carga_Horaria'] < 12.0 else f"🕒 Turno: {r['Turno']}", axis=1)
        df['Qtd_Caixa'] = df.groupby(['Posto', 'Caixa_Visual'])['Nome'].transform('count')

        for col in ['Atendimentos', 'GC', 'GA', 'S10 - A', 'ETANOL']:
            df[f'max_caixa_{col}'] = df.groupby(['Posto', 'Caixa_Visual'])[col].transform('max')
            df[f'max_carga_{col}'] = df.groupby(['Posto', 'Carga_Horaria'])[col].transform('max')
            df[f'{col} %'] = df.apply(lambda r, c=col: r[c] / (r[f'max_carga_{c}'] if r['Qtd_Caixa'] == 1 else r[f'max_caixa_{c}']) if (r[f'max_carga_{c}'] if r['Qtd_Caixa'] == 1 else r[f'max_caixa_{c}']) > 0 else 0.0, axis=1)

        df['Competição (Ref.)'] = df.apply(lambda r: "Equipe do Quadro" if r['Qtd_Caixa'] > 1 else f"Competindo com Quadro de {r['Carga_Horaria']}h", axis=1)

        def define_meta_ga(cargo):
            c_limpo = str(cargo).strip().upper()
            if c_limpo == "CX MANHÃ": return 1800.0
            elif c_limpo == "CX NOITE": return 2000.0
            else: return 4000.0
            
        df['Meta GA (Salva-Vidas)'] = df['Cargo'].apply(define_meta_ga)
        df['GC %'] = df.apply(lambda r: r['GC %'] + 0.10 if (r['GC %'] < 0.90 and r['GA'] >= r['Meta GA (Salva-Vidas)']) else r['GC %'], axis=1)
        df = df.sort_values(by=['Posto', 'Carga_Horaria', 'Turno'], ascending=[True, False, True])
        
    return df

def f_br(val): return "0,00" if pd.isna(val) else "{:,.2f}".format(val).replace(",", "X").replace(".", ",").replace("X", ".")
def f_int_br(val): return "0" if pd.isna(val) else "{:,.0f}".format(val).replace(",", ".")
def f_pct(val): return "0,00%" if pd.isna(val) else "{:.2f}%".format(val * 100).replace(".", ",")
def f_moeda(val): return "R$ 0,00" if pd.isna(val) else "R$ {:,.2f}".format(val).replace(",", "X").replace(".", ",").replace("X", ".")
def cor_style(val): return 'color: #10b981; font-weight: bold;' if val >= 0.90 else 'color: #ef4444; font-weight: bold;'

# ==========================================
# GESTÃO DE ACESSOS (SÓ PARA ADMIN)
# ==========================================
if menu == "🔐 Gestão de Acessos":
    st.header("🔐 Gestão de Usuários")
    st.markdown("Crie usuários e acompanhe o histórico de acessos da sua equipe.")
    
    aba_novo_u, aba_editar_u, aba_historico = st.tabs(["🆕 Novo Usuário", "📋 Editar / Inativar", "🕵️ Histórico de Logins"])
    
    with aba_novo_u:
        with st.container(border=True):
            with st.form("form_usuario", clear_on_submit=True):
                col1, col2 = st.columns(2)
                with col1:
                    novo_login = st.text_input("Login (Ex: gerente.joao)*").strip()
                    novo_perfil = st.selectbox("Perfil de Acesso", ["Admin", "Operador"], help="Admin tem acesso a esta tela. Operador acessa todo o resto, mas não cria usuários.")
                with col2:
                    nova_senha = st.text_input("Senha*", type="password").strip()
                    
                if st.form_submit_button("Criar Usuário", type="primary"):
                    if novo_login and nova_senha:
                        if novo_login in st.session_state['usuarios']['Usuario'].values:
                            st.error("Esse login já existe!")
                        else:
                            st.session_state['usuarios'] = pd.concat([st.session_state['usuarios'], pd.DataFrame({'Usuario': [novo_login], 'Senha': [nova_senha], 'Perfil': [novo_perfil], 'Status': ['Ativo']})], ignore_index=True)
                            salvar_dados()
                            st.success(f"✅ Usuário {novo_login} criado com sucesso!")
                            st.rerun()
                    else:
                        st.warning("Preencha o Login e a Senha.")

    with aba_editar_u:
        if st.session_state['usuarios'].empty:
            st.info("Nenhum usuário cadastrado no banco de dados ainda.")
        else:
            with st.container(border=True):
                user_editar = st.selectbox("Selecione o Usuário", st.session_state['usuarios']['Usuario'])
                dados_u = st.session_state['usuarios'][st.session_state['usuarios']['Usuario'] == user_editar].iloc[0]
                
                with st.form("form_editar_usuario"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        senha_u = st.text_input("Nova Senha", value=dados_u['Senha'])
                    with col2:
                        perfis = ["Admin", "Operador"]
                        perfil_u = st.selectbox("Perfil", perfis, index=perfis.index(dados_u['Perfil']) if dados_u['Perfil'] in perfis else 0)
                    with col3:
                        status_u = st.selectbox("Status", ["Ativo", "Inativo"], index=0 if dados_u['Status'] == 'Ativo' else 1)
                        
                    if st.form_submit_button("Atualizar Usuário"):
                        idx = st.session_state['usuarios'].index[st.session_state['usuarios']['Usuario'] == user_editar][0]
                        st.session_state['usuarios'].at[idx, 'Senha'] = senha_u.strip()
                        st.session_state['usuarios'].at[idx, 'Perfil'] = perfil_u
                        st.session_state['usuarios'].at[idx, 'Status'] = status_u
                        salvar_dados()
                        st.success("✅ Usuário atualizado!")
                        st.rerun()
            
            st.subheader("Usuários Cadastrados")
            st.dataframe(st.session_state['usuarios'][['Usuario', 'Perfil', 'Status']], use_container_width=True, hide_index=True)

    with aba_historico:
        st.subheader("Últimos Acessos ao Sistema")
        df_logs = st.session_state.get('log_acessos', pd.DataFrame())
        if df_logs.empty:
            st.info("Nenhum acesso registrado ainda. Os próximos logins aparecerão aqui.")
        else:
            st.dataframe(df_logs.iloc[::-1], use_container_width=True, hide_index=True)

# ==========================================
# TELAS DE CADASTRO
# ==========================================
elif menu == "🏢 Cadastro Empresa":
    st.header("🏢 Gestão de Empresas")
    aba_nova, aba_editar, aba_excluir = st.tabs(["🆕 Nova Empresa", "✏️ Editar Existente", "⛔ Inativar"])
    
    with aba_nova:
        with st.container(border=True):
            with st.form("form_empresa", clear_on_submit=True):
                novo_posto = st.text_input("Nome da Empresa (Igual ao cabeçalho do relatório)*").upper()
                if st.form_submit_button("Cadastrar Posto", type="primary") and novo_posto:
                    st.session_state['empresas'] = pd.concat([st.session_state['empresas'], pd.DataFrame({'Posto': [novo_posto], 'Status': ['Ativo']})], ignore_index=True)
                    salvar_dados()
                    st.success(f"✅ {novo_posto} cadastrado!")
                    st.rerun()
    
    with aba_editar:
        with st.container(border=True):
            if not st.session_state['empresas'].empty:
                emp_para_editar = st.selectbox("Selecione a Empresa", st.session_state['empresas']['Posto'])
                dados_emp = st.session_state['empresas'][st.session_state['empresas']['Posto'] == emp_para_editar].iloc[0]
                with st.form("form_editar_emp"):
                    novo_nome_emp = st.text_input("Nome da Empresa", value=dados_emp['Posto']).upper()
                    novo_status_emp = st.selectbox("Status", ["Ativo", "Inativo"], index=0 if dados_emp.get('Status', 'Ativo') == 'Ativo' else 1)
                    if st.form_submit_button("Atualizar Empresa"):
                        st.session_state['equipe'].loc[st.session_state['equipe']['Posto'] == emp_para_editar, 'Posto'] = novo_nome_emp
                        idx = st.session_state['empresas'].index[st.session_state['empresas']['Posto'] == emp_para_editar][0]
                        st.session_state['empresas'].at[idx, 'Posto'] = novo_nome_emp
                        st.session_state['empresas'].at[idx, 'Status'] = novo_status_emp
                        salvar_dados()
                        st.success("✅ Atualizado!")
                        st.rerun()

    with aba_excluir:
        with st.container(border=True):
            if not st.session_state['empresas'].empty:
                postos_ativos = st.session_state['empresas'][st.session_state['empresas']['Status'] == 'Ativo']['Posto']
                emp_inativar = st.selectbox("Inativar Empresa", postos_ativos if not postos_ativos.empty else ["Nenhuma"])
                if emp_inativar != "Nenhuma" and st.button("⛔ Inativar"):
                    idx = st.session_state['empresas'].index[st.session_state['empresas']['Posto'] == emp_inativar][0]
                    st.session_state['empresas'].at[idx, 'Status'] = 'Inativo'
                    salvar_dados()
                    st.rerun()

    if not st.session_state['empresas'].empty:
        st.subheader("Lista de Postos")
        st.dataframe(st.session_state['empresas'], use_container_width=True, hide_index=True)

elif menu == "⏰ Cadastro Turnos":
    st.header("⏰ Gestão de Turnos e Horários")
    aba_novo_t, aba_editar_t, aba_excluir_t = st.tabs(["🆕 Novo Turno", "✏️ Editar Turno", "⛔ Inativar Turno"])

    with aba_novo_t:
        with st.container(border=True):
            with st.form("form_turno", clear_on_submit=True):
                novo_turno = st.text_input("Descrição do Turno (Ex: 06h às 18h)*").upper()
                if st.form_submit_button("Criar Turno", type="primary") and novo_turno:
                    st.session_state['turnos'] = pd.concat([st.session_state['turnos'], pd.DataFrame({'Turno': [novo_turno], 'Status': ['Ativo']})], ignore_index=True)
                    salvar_dados()
                    st.rerun()

    with aba_editar_t:
        with st.container(border=True):
            if not st.session_state['turnos'].empty:
                turno_editar = st.selectbox("Selecione o Turno", st.session_state['turnos']['Turno'])
                dados_turno = st.session_state['turnos'][st.session_state['turnos']['Turno'] == turno_editar].iloc[0]
                with st.form("form_editar_turno"):
                    novo_nome_turno = st.text_input("Nome do Turno", value=dados_turno['Turno']).upper()
                    novo_status_turno = st.selectbox("Status", ["Ativo", "Inativo"], index=0 if dados_turno.get('Status', 'Ativo') == 'Ativo' else 1)
                    if st.form_submit_button("Atualizar Turno"):
                        st.session_state['equipe'].loc[st.session_state['equipe']['Turno'] == turno_editar, 'Turno'] = novo_nome_turno
                        idx = st.session_state['turnos'].index[st.session_state['turnos']['Turno'] == turno_editar][0]
                        st.session_state['turnos'].at[idx, 'Turno'] = novo_nome_turno
                        st.session_state['turnos'].at[idx, 'Status'] = novo_status_turno
                        salvar_dados()
                        st.rerun()

    with aba_excluir_t:
        with st.container(border=True):
            if not st.session_state['turnos'].empty:
                turnos_ativos = st.session_state['turnos'][st.session_state['turnos']['Status'] == 'Ativo']['Turno']
                turno_inativar = st.selectbox("Inativar Turno", turnos_ativos if not turnos_ativos.empty else ["Nenhum"])
                if turno_inativar != "Nenhum" and st.button("⛔ Inativar"):
                    idx = st.session_state['turnos'].index[st.session_state['turnos']['Turno'] == turno_inativar][0]
                    st.session_state['turnos'].at[idx, 'Status'] = 'Inativo'
                    salvar_dados()
                    st.rerun()

    if not st.session_state['turnos'].empty:
        st.subheader("Turnos Cadastrados")
        st.dataframe(st.session_state['turnos'], use_container_width=True, hide_index=True)

elif menu == "👤 Cadastro Colaborador":
    st.header("👤 Gestão de Colaboradores")
    aba_lista, aba_novo, aba_editar, aba_desligar = st.tabs(["📋 Lista de Colaboradores", "🆕 Novo Cadastro", "✏️ Editar Existente", "⛔ Desligar"])

    with aba_lista:
        if st.session_state['equipe'].empty: st.info("Nenhum colaborador cadastrado.")
        else: st.dataframe(st.session_state['equipe'], use_container_width=True, hide_index=True)

    with aba_novo:
        postos_ativos = st.session_state['empresas'][st.session_state['empresas']['Status'] == 'Ativo']['Posto']
        turnos_ativos = st.session_state['turnos'][st.session_state['turnos']['Status'] == 'Ativo']['Turno']
        if postos_ativos.empty or turnos_ativos.empty: st.warning("⚠️ Cadastre Empresas e Turnos ativos primeiro.")
        else:
            with st.container(border=True):
                with st.form("form_frentista", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        posto_f = st.selectbox("Posto", postos_ativos)
                        nome_f = st.text_input("Nome Completo*").upper()
                    with col2:
                        turno_f = st.selectbox("Turno", turnos_ativos) 
                        cargo_f = st.selectbox("Cargo", ["Frentista", "CX MANHÃ", "CX NOITE", "Gerente", "Chefe de Pista"])
                    if st.form_submit_button("Salvar Colaborador", type="primary") and nome_f:
                        st.session_state['equipe'] = pd.concat([st.session_state['equipe'], pd.DataFrame({'Posto': [posto_f], 'Turno': [turno_f], 'Cargo': [cargo_f], 'Nome': [nome_f], 'Status': ['Ativo']})], ignore_index=True)
                        salvar_dados()
                        st.rerun()

    with aba_editar:
        with st.container(border=True):
            if not st.session_state['equipe'].empty:
                colab_para_editar = st.selectbox("Selecione o Colaborador", st.session_state['equipe']['Nome'])
                dados_atuais = st.session_state['equipe'][st.session_state['equipe']['Nome'] == colab_para_editar].iloc[0]
                with st.form("form_edicao"):
                    col1, col2 = st.columns(2)
                    with col1:
                        novo_posto_e = st.selectbox("Posto", st.session_state['empresas']['Posto'], index=list(st.session_state['empresas']['Posto']).index(dados_atuais['Posto']))
                        novo_nome_e = st.text_input("Nome", value=dados_atuais['Nome']).upper()
                        novo_status_e = st.selectbox("Status", ["Ativo", "Inativo"], index=0 if dados_atuais.get('Status', 'Ativo') == 'Ativo' else 1)
                    with col2:
                        lista_turnos = list(st.session_state['turnos']['Turno'])
                        novo_turno_e = st.selectbox("Turno", lista_turnos, index=lista_turnos.index(dados_atuais['Turno']) if dados_atuais['Turno'] in lista_turnos else 0)
                        lista_cargos = ["Frentista", "CX MANHÃ", "CX NOITE", "Gerente", "Chefe de Pista"]
                        novo_cargo_e = st.selectbox("Cargo", lista_cargos, index=lista_cargos.index(dados_atuais['Cargo']) if dados_atuais['Cargo'] in lista_cargos else 0)
                    if st.form_submit_button("Atualizar Informações"):
                        idx = st.session_state['equipe'].index[st.session_state['equipe']['Nome'] == colab_para_editar][0]
                        st.session_state['equipe'].at[idx, 'Posto'] = novo_posto_e
                        st.session_state['equipe'].at[idx, 'Turno'] = novo_turno_e
                        st.session_state['equipe'].at[idx, 'Cargo'] = novo_cargo_e
                        st.session_state['equipe'].at[idx, 'Nome'] = novo_nome_e
                        st.session_state['equipe'].at[idx, 'Status'] = novo_status_e
                        salvar_dados()
                        st.rerun()

    with aba_desligar:
        with st.container(border=True):
            if not st.session_state['equipe'].empty:
                colabs_ativos = st.session_state['equipe'][st.session_state['equipe']['Status'] == 'Ativo']['Nome']
                colab_inativar = st.selectbox("Desligar Colaborador", colabs_ativos if not colabs_ativos.empty else ["Nenhum"])
                if colab_inativar != "Nenhum" and st.button("⛔ Inativar Colaborador"):
                    idx = st.session_state['equipe'].index[st.session_state['equipe']['Nome'] == colab_inativar][0]
                    st.session_state['equipe'].at[idx, 'Status'] = 'Inativo'
                    salvar_dados()
                    st.rerun()

# ==========================================
# TELA: IMPORTAR PLANILHAS
# ==========================================
elif menu == "📈 Importar Planilhas":
    st.header("📈 Importação de Resultados")
    col_u, col_h = st.columns([1.5, 1])
    
    with col_u:
        with st.container(border=True):
            arquivos = st.file_uploader("Suba as planilhas (Vendas, Produtos ou Horários)", type=["xlsx", "xls", "csv"], accept_multiple_files=True)
            if arquivos:
                if st.button(f"🚀 Processar {len(arquivos)} Arquivos", type="primary"):
                    ids_existentes = [item['id'] for item in st.session_state['processados_list']]
                    for arq in arquivos:
                        id_arq = f"{arq.name}_{arq.size}"
                        if id_arq in ids_existentes: continue
                        try:
                            arq.seek(0)
                            df_b = pd.read_excel(arq, header=None) if arq.name.endswith(('.xlsx', '.xls')) else pd.read_csv(arq, header=None, sep=None, engine='python', encoding='utf-8-sig')
                            
                            linha_cab, tipo_rel, col_alvo, item_nome, mes_ref = None, None, None, "", "DESCONHECIDO"

                            for idx, row in df_b.head(30).iterrows():
                                l_str = " ".join([str(v).upper() for v in row.values if pd.notna(v)])
                                if 'PERÍODO:' in l_str or 'PERIODO:' in l_str:
                                    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', l_str)
                                    if m: mes_ref = f"{m.group(2)}/{m.group(3)}"
                                    
                                if 'ATENDENTE' in l_str and any(x in l_str for x in ['NR. VENDAS', 'Nº VENDAS']):
                                    linha_cab, tipo_rel, col_alvo, item_nome = idx, "ATEND", "Atendimentos", "ATENDIMENTOS"
                                elif 'ATENDENTE' in l_str and 'QUANTIDADE' in l_str:
                                    linha_cab, tipo_rel = idx, "COMB"
                                elif 'HORA' in l_str and 'LITRAGEM' in l_str:
                                    linha_cab, tipo_rel, item_nome = idx, "METAS", "METAS CAIXA (SINTÉTICO)"

                            if tipo_rel == "COMB":
                                for idx, row in df_b.head(30).iterrows():
                                    t = ' '.join([str(v) for v in row.values if pd.notna(v)]).upper()
                                    if 'IDENTIFICAÇÃO DO ITEM' in t:
                                        item_nome = t.split(':', 1)[1].split('-', 1)[-1].strip()
                                        break
                                if 'COMUM' in item_nome: col_alvo = 'GC'
                                elif 'ADITIVADA' in item_nome: col_alvo = 'GA'
                                elif 'S10' in item_nome: col_alvo = 'S10 - A'
                                elif 'ETANOL' in item_nome: col_alvo = 'ETANOL'

                            if linha_cab is not None:
                                arq.seek(0)
                                df_l = pd.read_excel(arq, header=linha_cab) if arq.name.endswith(('.xlsx', '.xls')) else pd.read_csv(arq, header=linha_cab, sep=None, engine='python', encoding='utf-8-sig')
                                
                                if tipo_rel in ["ATEND", "COMB"] and col_alvo:
                                    df_l.columns = df_l.columns.astype(str).str.strip().str.upper().str.replace('\n', ' ')
                                    c_at = [c for c in df_l.columns if 'ATENDENTE' in c][0]
                                    c_vl = [c for c in df_l.columns if any(x in c for x in ['NR. VENDAS', 'Nº VENDAS'])][0] if tipo_rel == "ATEND" else [c for c in df_l.columns if 'QUANTIDADE' in c][0]
                                    
                                    df_f = df_l[[c_at, c_vl]].dropna().copy()
                                    df_f.rename(columns={c_at: 'Nome', c_vl: 'Val'}, inplace=True)
                                    df_f['Nome'] = df_f['Nome'].apply(lambda x: re.sub(r'^\s*\d+\s*-\s*', '', str(x)).replace('-', '').strip().upper())
                                    
                                    df_f['Val'] = df_f['Val'].apply(lambda v: float(str(v).replace('.', '').replace(',', '.')) if not isinstance(v, (int, float)) else float(v))
                                    for _, r in df_f.iterrows():
                                        nova_l = {'Arquivo': id_arq, 'Nome': r['Nome'], 'Mes': mes_ref, 'Atendimentos': 0, 'GC': 0, 'GA': 0, 'S10 - A': 0, 'ETANOL': 0}
                                        nova_l[col_alvo] = r['Val']
                                        st.session_state['vendas'] = pd.concat([st.session_state['vendas'], pd.DataFrame([nova_l])], ignore_index=True)
                                    
                                    st.session_state['processados_list'].append({'id': id_arq, 'Arquivo': arq.name, 'Mês': mes_ref, 'Tipo': item_nome})
                                    salvar_dados()
                                    st.success(f"✅ {item_nome} processado!")
                                    
                                elif tipo_rel == "METAS":
                                    col_hora_name, col_pct_name = None, None
                                    cols = list(df_l.columns)
                                    for i, c in enumerate(cols):
                                        if 'HORA' in str(c).upper().strip() and not col_hora_name: col_hora_name = c
                                        if 'LITRAGEM' in str(c).upper().strip() and i + 1 < len(cols):
                                            col_pct_name = cols[i+1] 
                                            break
                                                
                                    if col_hora_name and col_pct_name:
                                        df_l['H_INT'] = df_l[col_hora_name].apply(lambda v: int(re.search(r'\d+', str(v)).group()) if re.search(r'\d+', str(v)) else -1)
                                        df_l['P_VAL'] = df_l[col_pct_name].apply(lambda v: float(str(v).replace('%','').replace(',','.')) if pd.notna(v) else 0.0)
                                        
                                        if df_l['P_VAL'].sum() <= 2.0: df_l['P_VAL'] = df_l['P_VAL'] * 100.0
                                            
                                        soma_manha = df_l[(df_l['H_INT'] >= 4) & (df_l['H_INT'] <= 16)]['P_VAL'].sum()
                                        soma_noite = df_l[(df_l['H_INT'] >= 17) & (df_l['H_INT'] <= 23)]['P_VAL'].sum()
                                        
                                        st.session_state['config']['Meta_Dia'] = soma_manha / 2.0
                                        st.session_state['config']['Meta_Noite'] = soma_noite / 2.0
                                        st.session_state['processados_list'].append({'id': id_arq, 'Arquivo': arq.name, 'Mês': mes_ref, 'Tipo': item_nome})
                                        salvar_dados()
                                        st.success(f"✅ {item_nome} importado! Metas atualizadas.")
                        except Exception as e: st.error(f"Erro em {arq.name}: {e}")

    with col_h:
        with st.container(border=True):
            st.subheader("📜 Histórico")
            if st.session_state['processados_list']:
                c1, c2, c3, c4 = st.columns([4, 2, 3, 1])
                c1.markdown("**Arquivo**"); c2.markdown("**Mês**"); c3.markdown("**Tipo**")
                st.markdown("<hr style='margin: 0.5em 0;'>", unsafe_allow_html=True)
                
                for item in st.session_state['processados_list']:
                    c1, c2, c3, c4 = st.columns([4, 2, 3, 1])
                    c1.caption(item['Arquivo']); c2.caption(item['Mês']); c3.caption(item['Tipo'])
                    if c4.button("❌", key=f"del_{item['id']}", help="Remover"):
                        st.session_state['processados_list'] = [x for x in st.session_state['processados_list'] if x['id'] != item['id']]
                        st.session_state['vendas'] = st.session_state['vendas'][st.session_state['vendas']['Arquivo'] != item['id']]
                        salvar_dados()
                        st.rerun()

                st.markdown("---")
                if st.button("🧹 Limpar TODAS as Importações", use_container_width=True):
                    st.session_state['processados_list'] = []
                    st.session_state['vendas'] = pd.DataFrame(columns=['Arquivo', 'Nome', 'Mes', 'Atendimentos', 'GC', 'GA', 'S10 - A', 'ETANOL'])
                    salvar_dados()
                    st.rerun()
            else:
                st.info("Nenhum arquivo no histórico.")

# ==========================================
# TELA: PAINEL GERAL
# ==========================================
elif menu == "📊 Painel Geral":
    st.header("📊 Dashboard Operacional")
    
    with st.container(border=True):
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            meses_disp = sorted(st.session_state['vendas']['Mes'].unique(), reverse=True)
            mes_sel = st.selectbox("Selecione o Mês", meses_disp if meses_disp else ["Sem Vendas"])
        with col_f2:
            posto_sel = st.selectbox("Filtrar Posto", ["Todos"] + list(st.session_state['empresas']['Posto']))

    df = calcular_dataframe_resultados(mes_sel, posto_sel)

    if not df.empty:
        colunas_tabela = ['Turno', 'Cargo', 'Nome', 'Atendimentos', 'Atendimentos %', 'Litragem', 'GC', 'GC %', 'GA', 'Meta GA (Salva-Vidas)', 'GA %', 'S10 - A', 'S10 - A %', 'ETANOL', 'ETANOL %']

        for posto in sorted(df['Posto'].unique()):
            st.subheader(f"🏢 {posto}")
            df_posto = df[df['Posto'] == posto]
            
            for caixa in df_posto['Caixa_Visual'].unique():
                df_caixa = df_posto[df_posto['Caixa_Visual'] == caixa]
                
                with st.container(border=True):
                    qtd_equipe = len(df_caixa)
                    ref_texto = df_caixa['Competição (Ref.)'].iloc[0]
                    
                    if "Agrupados" in caixa:
                        turnos_misturados = " / ".join(sorted(df_caixa['Turno'].unique()))
                        st.markdown(f"**{caixa}:** {turnos_misturados} &nbsp;&nbsp;|&nbsp;&nbsp; 👥 {qtd_equipe} Colaboradores")
                    else:
                        st.markdown(f"**{caixa}** &nbsp;&nbsp;|&nbsp;&nbsp; **⚖️ {ref_texto}** &nbsp;&nbsp;|&nbsp;&nbsp; 👥 {qtd_equipe} Colaborador(es)")
                    
                    st.dataframe(
                        df_caixa[colunas_tabela].style.map(cor_style, subset=[c for c in colunas_tabela if '%' in c]).format({
                            'Atendimentos': f_int_br, 'Litragem': f_br, 'GC': f_br, 'GA': f_br, 'Meta GA (Salva-Vidas)': f_br, 'S10 - A': f_br, 'ETANOL': f_br,
                            'Atendimentos %': f_pct, 'GC %': f_pct, 'GA %': f_pct, 'S10 - A %': f_pct, 'ETANOL %': f_pct
                        }), 
                        use_container_width=True, hide_index=True
                    )
            
            st.markdown(f"**Resumo de Desempenho - {posto}**")
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("👥 Atendimentos", f_int_br(df_posto['Atendimentos'].sum()))
            c2.metric("⛽ Litros (Total)", f_br(df_posto['Litragem'].sum()))
            c3.metric("🟡 Comum (GC)", f_br(df_posto['GC'].sum()))
            c4.metric("🔴 Aditivada (GA)", f_br(df_posto['GA'].sum()))
            c5.metric("⚫ S10 - A", f_br(df_posto['S10 - A'].sum()))
            c6.metric("🟢 Etanol", f_br(df_posto['ETANOL'].sum()))
            st.markdown("---")

        if posto_sel == "Todos" and len(df['Posto'].unique()) > 1:
            st.subheader("🏆 Resumo Global da Rede (Todos os Postos)")
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("👥 Atend. Totais", f_int_br(df['Atendimentos'].sum()))
            c2.metric("⛽ Litros Totais", f_br(df['Litragem'].sum()))
            c3.metric("🟡 Comum Total", f_br(df['GC'].sum()))
            c4.metric("🔴 Aditivada Total", f_br(df['GA'].sum()))
            c5.metric("⚫ S10 - A Total", f_br(df['S10 - A'].sum()))
            c6.metric("🟢 Etanol Total", f_br(df['ETANOL'].sum()))

        st.markdown("<br>", unsafe_allow_html=True)
        col_pdf, col_excel = st.columns(2)
        df_export = df[['Posto'] + colunas_tabela].copy()
        
        for col, func in {'Atendimentos': f_int_br, 'Atendimentos %': f_pct, 'Litragem': f_br, 'GC': f_br, 'GC %': f_pct, 'GA': f_br, 'Meta GA (Salva-Vidas)': f_br, 'GA %': f_pct, 'S10 - A': f_br, 'S10 - A %': f_pct, 'ETANOL': f_br, 'ETANOL %': f_pct}.items():
            if col in df_export.columns: df_export[col] = df_export[col].apply(func)
        
        with col_pdf:
            if HAS_REPORTLAB:
                pdf_bytes = gerar_pdf(df_export, f"Painel Operacional - {mes_sel}", agrupar_por='Posto', texto_total="colaboradores")
                if pdf_bytes: st.download_button("📄 Baixar Painel (PDF)", data=pdf_bytes, file_name=f"Painel_{mes_sel}.pdf", mime="application/pdf", type="primary")
            else: st.warning("⚠️ Instale: `pip install reportlab`")
        with col_excel:
            st.download_button("📊 Baixar Painel (Excel)", data=gerar_excel(df_export, agrupar_por='Posto'), file_name=f"Painel_{mes_sel}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Nenhum colaborador ativo. Acesse o menu 'Cadastro Colaborador'.")

# ==========================================
# TELA: BONIFICAÇÃO
# ==========================================
elif menu == "💰 Bonificação":
    st.header("💰 Painel Financeiro e Comissões")

    pct_dia_atual = float(st.session_state['config']['Meta_Dia'].iloc[0])
    pct_noite_atual = float(st.session_state['config']['Meta_Noite'].iloc[0])

    with st.container(border=True):
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)
        with col_f1:
            meses_disp = sorted(st.session_state['vendas']['Mes'].unique(), reverse=True)
            mes_sel_b = st.selectbox("📅 Mês", meses_disp if meses_disp else ["Sem Vendas"], key='mes_boni')
        with col_f2:
            posto_sel_b = st.selectbox("🏢 Unidade", ["Todos"] + list(st.session_state['empresas']['Posto']), key='posto_boni')
        with col_f3:
            pct_caixa_dia = st.number_input("☀️ Meta CX MANHÃ (%)", value=pct_dia_atual, step=0.01, format="%.2f")
        with col_f4:
            pct_caixa_noite = st.number_input("🌙 Meta CX NOITE (%)", value=pct_noite_atual, step=0.01, format="%.2f")

    if pct_caixa_dia != pct_dia_atual or pct_caixa_noite != pct_noite_atual:
        st.session_state['config']['Meta_Dia'] = pct_caixa_dia
        st.session_state['config']['Meta_Noite'] = pct_caixa_noite
        salvar_dados()

    df_boni = calcular_dataframe_resultados(mes_sel_b, posto_sel_b)

    if not df_boni.empty:
        tot_litros = df_boni['Litragem'].sum()
        valor_bonificacao_total = tot_litros * 0.006 
        valor_bonus_caixa_total = tot_litros * 0.0025 
        valor_bonificacao_ga_total = df_boni['GA'].sum() * 0.012 
        
        st.markdown("<br>", unsafe_allow_html=True)
        col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
        col_kpi1.metric("⛽ Volume Base", f"{f_br(tot_litros)} L")
        col_kpi2.metric("💰 Fundo Pista (0,006/L)", f_moeda(valor_bonificacao_total))
        col_kpi3.metric("💵 Fundo Caixa Bruto (0,25%)", f_moeda(valor_bonus_caixa_total))
        col_kpi4.metric("🔴 Total Prêmio Aditivada", f_moeda(valor_bonificacao_ga_total))

        tot_atend = df_boni['Atendimentos'].sum()
        df_boni['Part. Atendimentos (%)'] = df_boni['Atendimentos'] / tot_atend if tot_atend > 0 else 0.0
        df_boni['💰 Bonificação (R$)'] = df_boni['Part. Atendimentos (%)'] * valor_bonificacao_total
        df_boni['Part. Litragem (%)'] = df_boni['Litragem'] / tot_litros if tot_litros > 0 else 0.0
        df_boni['Mix (GC + GA)'] = df_boni['GC'] + df_boni['GA']
        df_boni['Participação GC (%)'] = df_boni.apply(lambda r: r['GC'] / r['Litragem'] if r['Litragem'] > 0 else 0.0, axis=1)
        df_boni['💰 Bonificação GA (R$)'] = df_boni['GA'] * 0.012
        df_boni['Litragem_Posto'] = df_boni.groupby('Posto')['Litragem'].transform('sum')
        
        def calcular_bonus_caixa(row):
            cargo = str(row['Cargo']).upper().strip()
            fundo_posto = row['Litragem_Posto'] * 0.0025
            if 'MANHÃ' in cargo or cargo == 'CX DIA': return (fundo_posto * (pct_caixa_dia / 100)) / 2
            elif 'NOITE' in cargo: return (fundo_posto * (pct_caixa_noite / 100)) / 2
            elif 'CX' in cargo or 'CAIXA' in cargo: return (fundo_posto * (pct_caixa_dia / 100)) / 2
            else: return 0.0

        df_boni['💰 Bônus Caixa (R$)'] = df_boni.apply(calcular_bonus_caixa, axis=1)
        df_boni['💰 Total a Receber (R$)'] = df_boni['💰 Bonificação (R$)'] + df_boni['💰 Bonificação GA (R$)'] + df_boni['💰 Bônus Caixa (R$)']

        colunas_boni = ['Posto', 'Turno', 'Nome', 'Cargo', 'Atendimentos', 'Part. Atendimentos (%)', '💰 Bonificação (R$)', 'Litragem', 'Part. Litragem (%)', 'Mix (GC + GA)', 'GC', 'Participação GC (%)', 'GA', '💰 Bonificação GA (R$)', '💰 Bônus Caixa (R$)', '💰 Total a Receber (R$)']
        
        st.subheader("📝 Folha de Pagamento Detalhada")
        with st.container(border=True):
            st.dataframe(
                df_boni[colunas_boni].style.format({
                    'Atendimentos': f_int_br, 'Part. Atendimentos (%)': f_pct, '💰 Bonificação (R$)': f_moeda,
                    'Litragem': f_br, 'Part. Litragem (%)': f_pct, 'Mix (GC + GA)': f_br, 'GC': f_br,
                    'Participação GC (%)': f_pct, 'GA': f_br, '💰 Bonificação GA (R$)': f_moeda,
                    '💰 Bônus Caixa (R$)': f_moeda, '💰 Total a Receber (R$)': f_moeda
                }), use_container_width=True, hide_index=True
            )
            
        st.markdown("<br>", unsafe_allow_html=True)
        col_pdf, col_excel = st.columns(2)
        df_export = df_boni[colunas_boni].copy()
        
        for col, func in {'Atendimentos': f_int_br, 'Part. Atendimentos (%)': f_pct, '💰 Bonificação (R$)': f_moeda, 'Litragem': f_br, 'Part. Litragem (%)': f_pct, 'Mix (GC + GA)': f_br, 'GC': f_br, 'Participação GC (%)': f_pct, 'GA': f_br, '💰 Bonificação GA (R$)': f_moeda, '💰 Bônus Caixa (R$)': f_moeda, '💰 Total a Receber (R$)': f_moeda}.items():
            if col in df_export.columns: df_export[col] = df_export[col].apply(func)
                
        with col_pdf:
            if HAS_REPORTLAB:
                pdf_bytes = gerar_pdf(df_export, f"Folha de Pagamento - {mes_sel_b}", agrupar_por='Posto', texto_total="colaboradores")
                if pdf_bytes: st.download_button("📄 Baixar Folha (PDF)", data=pdf_bytes, file_name=f"Folha_{mes_sel_b}.pdf", mime="application/pdf", type="primary")
            else: st.warning("⚠️ Instale: `pip install reportlab`")
        with col_excel:
            st.download_button("📊 Baixar Folha (Excel)", data=gerar_excel(df_export, agrupar_por='Posto'), file_name=f"Folha_{mes_sel_b}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Nenhum colaborador ativo.")

# ==========================================
# TELA: ANIVERSARIANTES
# ==========================================
elif menu == "🎂 Aniversariantes":
    st.header("🎂 Painel de Aniversariantes")
    aba_lista, aba_importar = st.tabs(["🎉 Lista do Mês", "📥 Importar Base"])
    
    with aba_lista:
        df_niver = st.session_state['aniversarios'].copy()
        if not df_niver.empty:
            df_equipe = st.session_state['equipe'][['Nome', 'Posto']].copy()
            df_equipe['Nome'] = df_equipe['Nome'].astype(str).str.strip().str.upper()
            df_equipe = df_equipe.drop_duplicates(subset=['Nome'], keep='last')
            
            df_niver = pd.merge(df_niver, df_equipe, on='Nome', how='left', suffixes=('', '_eq'))
            df_niver['Posto'] = df_niver.apply(lambda r: r['Posto_eq'] if (pd.notna(r['Posto_eq']) and r['Posto'] == 'Não Vinculado') else r['Posto'], axis=1)
            df_niver['Posto'] = df_niver['Posto'].fillna('Não Vinculado') 
            df_niver['Data_DT'] = pd.to_datetime(df_niver['Data de Nascimento'], errors='coerce', dayfirst=True)
            df_niver = df_niver.dropna(subset=['Data_DT']).copy()
            
            hoje = datetime.today()
            df_niver['Idade Hoje'] = df_niver['Data_DT'].apply(lambda nasc: hoje.year - nasc.year - ((hoje.month, hoje.day) < (nasc.month, nasc.day)))
            df_niver['Mês'] = df_niver['Data_DT'].dt.month
            df_niver['Dia'] = df_niver['Data_DT'].dt.day
            nomes_meses = {1:'Janeiro', 2:'Fevereiro', 3:'Março', 4:'Abril', 5:'Maio', 6:'Junho', 7:'Julho', 8:'Agosto', 9:'Setembro', 10:'Outubro', 11:'Novembro', 12:'Dezembro'}
            df_niver['Mês Nome'] = df_niver['Mês'].map(nomes_meses)
            
            with st.container(border=True):
                c1, c2, c3 = st.columns(3)
                lista_meses = ["Todos"] + [nomes_meses[m] for m in sorted(df_niver['Mês'].unique())]
                mes_filtro = c1.selectbox("Mês", lista_meses, index=lista_meses.index(nomes_meses.get(hoje.month, "Todos")) if nomes_meses.get(hoje.month, "Todos") in lista_meses else 0)
                posto_filtro = c2.selectbox("Empresa", ["Todos"] + sorted([str(x) for x in df_niver['Posto'].unique()]))
                genero_filtro = c3.selectbox("Gênero", ["Todos"] + sorted([str(x) for x in df_niver['Gênero'].unique()]))
            
            if mes_filtro != "Todos": df_niver = df_niver[df_niver['Mês Nome'] == mes_filtro]
            if posto_filtro != "Todos": df_niver = df_niver[df_niver['Posto'] == posto_filtro]
            if genero_filtro != "Todos": df_niver = df_niver[df_niver['Gênero'] == genero_filtro]
            
            df_niver = df_niver.sort_values(by=['Mês', 'Dia'])
            
            if not df_niver.empty:
                st.dataframe(df_niver[['Posto', 'Nome', 'Gênero', 'Data de Nascimento', 'Idade Hoje', 'Mês Nome']].style.format({'Idade Hoje': '{:.0f} anos'}), use_container_width=True, hide_index=True)
                st.markdown("<br>", unsafe_allow_html=True)
                col_pdf, col_excel = st.columns(2)
                df_export = df_niver[['Posto', 'Nome', 'Gênero', 'Data de Nascimento', 'Idade Hoje', 'Mês Nome']].copy()
                df_export['Idade Hoje'] = df_export['Idade Hoje'].astype(str) + " anos"
                
                with col_pdf:
                    if HAS_REPORTLAB:
                        pdf_bytes = gerar_pdf(df_export, f"Aniversariantes - {mes_filtro}", agrupar_por='Posto', texto_total="funcionários")
                        if pdf_bytes: st.download_button("📄 Baixar Relatório (PDF)", data=pdf_bytes, file_name=f"Niver_{mes_filtro}.pdf", mime="application/pdf", type="primary")
                with col_excel:
                    st.download_button("📊 Baixar Relatório (Excel)", data=gerar_excel(df_export, agrupar_por='Posto'), file_name=f"Niver_{mes_filtro}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else: st.info("Nenhum aniversariante para estes filtros.")
        else: st.info("A base está vazia.")

    with aba_importar:
        with st.container(border=True):
            posto_lote = st.selectbox("Vinculação de Empresa:", ["🔍 Detectar da Planilha / Abas do Excel"] + list(st.session_state['empresas']['Posto']))
            arq_niver = st.file_uploader("Selecione a planilha", type=["xlsx", "xls", "csv"])
            
            if arq_niver and st.button("🚀 Processar", type="primary"):
                try:
                    lista_dfs = []
                    is_excel = arq_niver.name.endswith(('.xlsx', '.xls'))
                    sheet_names = pd.ExcelFile(arq_niver).sheet_names if is_excel else ['CSV_Unico']
                    
                    for sheet in sheet_names:
                        df_teste = pd.read_excel(arq_niver, sheet_name=sheet, header=None) if is_excel else pd.read_csv(arq_niver, header=None, sep=None, engine='python', encoding='utf-8-sig')
                        linha_cab = next((idx for idx, row in df_teste.head(20).iterrows() if 'NOME' in " ".join([str(v).upper() for v in row.values if pd.notna(v)]) and ('DATA' in " ".join([str(v).upper() for v in row.values if pd.notna(v)]) or 'NASC' in " ".join([str(v).upper() for v in row.values if pd.notna(v)]))), None)
                                
                        if linha_cab is not None:
                            df_imp = pd.read_excel(arq_niver, sheet_name=sheet, header=linha_cab) if is_excel else pd.read_csv(arq_niver, header=linha_cab, sep=None, engine='python', encoding='utf-8-sig')
                            c_n, c_d, c_p, c_g = None, None, None, None
                            for c in df_imp.columns:
                                c_up = str(c).upper().strip()
                                if 'NOME' in c_up and not c_n: c_n = c
                                elif ('DATA' in c_up or 'NASC' in c_up) and not c_d: c_d = c
                                elif ('EMPRESA' in c_up or 'POSTO' in c_up) and not c_p: c_p = c
                                elif ('SEXO' in c_up or 'GENERO' in c_up or 'GÊNERO' in c_up) and not c_g: c_g = c
                                
                            if c_n and c_d:
                                cols = [c_n, c_d] + ([c_p] if c_p else []) + ([c_g] if c_g else [])
                                df_novo = df_imp[cols].dropna(subset=[c_n, c_d]).copy()
                                df_novo.rename(columns={c_n: 'Nome', c_d: 'Data de Nascimento', **({c_p: 'Posto_P'} if c_p else {}), **({c_g: 'Gen_P'} if c_g else {})}, inplace=True)
                                
                                df_novo['Data de Nascimento'] = pd.to_datetime(df_novo['Data de Nascimento'], errors='coerce', dayfirst=True).dt.strftime('%d/%m/%Y')
                                df_novo.dropna(subset=['Data de Nascimento'], inplace=True)
                                df_novo['Nome'] = df_novo['Nome'].astype(str).str.strip().str.upper()
                                df_novo.drop_duplicates(subset=['Nome'], keep='last', inplace=True)
                                
                                if 'Gen_P' in df_novo.columns:
                                    df_novo['Gênero'] = df_novo['Gen_P'].apply(lambda g: 'MASCULINO' if str(g).strip().upper() in ['M', 'MASCULINO', 'MASC', 'HOMEM'] else ('FEMININO' if str(g).strip().upper() in ['F', 'FEMININO', 'FEM', 'MULHER'] else 'NÃO INFORMADO'))
                                    df_novo.drop(columns=['Gen_P'], inplace=True)
                                else: df_novo['Gênero'] = 'NÃO INFORMADO'
                                
                                if posto_lote != "🔍 Detectar da Planilha / Abas do Excel": df_novo['Posto'] = posto_lote
                                else: df_novo['Posto'] = str(sheet).strip().upper() if is_excel else (df_novo['Posto_P'].astype(str).str.strip().str.upper() if 'Posto_P' in df_novo.columns else 'Não Vinculado')
                                if 'Posto_P' in df_novo.columns: df_novo.drop(columns=['Posto_P'], inplace=True)
                                
                                lista_dfs.append(df_novo)

                    if lista_dfs:
                        df_final = pd.concat(lista_dfs)
                        st.session_state['aniversarios'] = pd.concat([st.session_state['aniversarios'], df_final]).drop_duplicates(subset=['Nome'], keep='last')
                        salvar_dados()
                        st.success("✅ Registros processados e salvos com sucesso!")
                        st.rerun()
                    else: st.error("Tabela não encontrada.")
                except Exception as e: st.error(f"Erro: {e}")
                
            st.markdown("---")
            if not st.session_state['aniversarios'].empty and st.button("🗑️ Limpar toda a base"):
                st.session_state['aniversarios'] = pd.DataFrame(columns=['Posto', 'Nome', 'Gênero', 'Data de Nascimento'])
                salvar_dados()
                st.rerun()