import sys
import subprocess

# --- AUTO-INSTALAÇÃO DE BIBLIOTECAS (Correção de Erros) ---
try:
    import google.oauth2
    import googleapiclient
except ImportError:
    import streamlit as st
    st.warning("⚠️ A instalar bibliotecas da Google em falta... Aguarda um momento.")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "google-api-python-client", "google-auth", "google-auth-oauthlib", "google-auth-httplib2"])
    st.success("Bibliotecas instaladas! Por favor reinicia a app.")
    st.stop()
# -----------------------------------------------------------

import streamlit as st
import sqlite3
import pandas as pd
import hashlib
import json
from datetime import datetime, timedelta, date, time
from streamlit_calendar import calendar
from fpdf import FPDF
import tempfile
import os
from PIL import Image
import io
import shutil

# --- BIBLIOTECAS GOOGLE DRIVE ---
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# ==========================================
# 1. CONFIGURAÇÃO E GOOGLE DRIVE SYNC (V44)
# ==========================================
st.set_page_config(page_title="GK Manager Pro V44", layout="wide", page_icon="🧤")

# Nome do ficheiro da base de dados
DB_FILE = 'gk_master_v38.db'
SCOPES = ['https://www.googleapis.com/auth/drive']

# --- FUNÇÕES DE GOOGLE DRIVE ---
def get_drive_service():
    """Autentica no Google Drive usando os segredos do Streamlit."""
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"], scopes=SCOPES)
            return build('drive', 'v3', credentials=creds)
        else:
            return None
    except Exception as e:
        st.error(f"Erro de Configuração Drive: {e}")
        return None

def sync_download_db():
    """Ao iniciar, tenta baixar a versão mais recente do Drive."""
    service = get_drive_service()
    folder_id = st.secrets["drive"]["folder_id"] if "drive" in st.secrets else None
    
    if service and folder_id:
        try:
            # Procura o ficheiro na pasta
            query = f"'{folder_id}' in parents and name = '{DB_FILE}' and trashed = false"
            results = service.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
            files = results.get('files', [])
            
            if files:
                file_id = files[0]['id']
                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                
                # Guarda localmente
                with open(DB_FILE, "wb") as f:
                    f.write(fh.getbuffer())
                print("Base de dados sincronizada do Drive com sucesso.")
        except Exception as e:
            print(f"Aviso: Não foi possível baixar do Drive (usando versão local). Erro: {e}")

def backup_to_drive():
    """Envia a base de dados local para o Google Drive."""
    service = get_drive_service()
    folder_id = st.secrets["drive"]["folder_id"] if "drive" in st.secrets else None
    
    if service and folder_id and os.path.exists(DB_FILE):
        try:
            # Verifica se já existe para atualizar
            query = f"'{folder_id}' in parents and name = '{DB_FILE}' and trashed = false"
            results = service.files().list(q=query, fields="files(id)").execute()
            files = results.get('files', [])
            
            media = MediaFileUpload(DB_FILE, mimetype='application/x-sqlite3', resumable=True)
            
            if files:
                # Atualiza o existente
                file_id = files[0]['id']
                service.files().update(fileId=file_id, media_body=media).execute()
                st.toast("✅ Backup automático no Drive concluído!", icon="☁️")
            else:
                # Cria novo
                file_metadata = {'name': DB_FILE, 'parents': [folder_id]}
                service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                st.toast("✅ Backup automático no Drive concluído!", icon="☁️")
                
        except Exception as e:
            st.error(f"⚠️ Erro ao fazer backup no Drive: {e}")

# Tenta sincronizar ao arrancar a app (uma vez por sessão)
if 'drive_synced' not in st.session_state:
    with st.spinner("A conectar ao Google Drive..."):
        sync_download_db()
    st.session_state['drive_synced'] = True

# ==========================================
# 2. BASE DE DADOS E SETUP
# ==========================================
def get_db_connection():
    conn = sqlite3.connect(DB_FILE) 
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_db_updates():
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. Tabela de Utilizadores
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)''')
    
    # 2. Tabela de Atletas
    c.execute('''CREATE TABLE IF NOT EXISTS goalkeepers (
                    id INTEGER PRIMARY KEY, user_id TEXT, name TEXT, age INTEGER, status TEXT, notes TEXT,
                    height REAL, wingspan REAL, arm_len_left REAL, arm_len_right REAL, glove_size TEXT,
                    jump_front_2 REAL, jump_front_l REAL, jump_front_r REAL, jump_lat_l REAL, jump_lat_r REAL,
                    test_res TEXT, test_agil TEXT, test_vel TEXT)''')
    
    # 3. Tabela de Exercícios
    c.execute('''CREATE TABLE IF NOT EXISTS exercises (
                    id INTEGER PRIMARY KEY, user_id TEXT, title TEXT, moment TEXT, training_type TEXT, 
                    description TEXT, objective TEXT, materials TEXT, space TEXT, image BLOB)''')
    
    # 4. Tabela de Sessões (Treinos/Jogos)
    c.execute('''CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY, user_id TEXT, type TEXT, title TEXT, start_date TEXT, drills_list TEXT, report TEXT)''')
    
    # 5. Tabela de Microciclos
    c.execute('''CREATE TABLE IF NOT EXISTS microcycles (
                    id INTEGER PRIMARY KEY, user_id TEXT, title TEXT, start_date TEXT, goal TEXT, report TEXT)''')
    
    # 6. Tabela de Avaliações
    c.execute('''CREATE TABLE IF NOT EXISTS training_ratings (
                    id INTEGER PRIMARY KEY, user_id TEXT, date TEXT, gk_id INTEGER, rating INTEGER, notes TEXT)''')
    
    # 7. Tabela de Presenças
    c.execute('''CREATE TABLE IF NOT EXISTS attendance (
                    id INTEGER PRIMARY KEY, session_id INTEGER, gk_id INTEGER, status TEXT)''')

    # 8. Tabela de Jogos (COMPLETA - 72 CAMPOS)
    c.execute('''CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY, 
                    user_id TEXT, date TEXT, opponent TEXT, gk_id INTEGER, goals_conceded INTEGER, saves INTEGER, result TEXT, report TEXT, rating INTEGER, 
                    
                    -- BLOQUEIOS (6)
                    db_bloq_sq_rast INTEGER, db_bloq_sq_med INTEGER, db_bloq_sq_alt INTEGER, db_bloq_cq_rast INTEGER, db_bloq_cq_med INTEGER, db_bloq_cq_alt INTEGER, 
                    
                    -- RECEÇÕES (6)
                    db_rec_sq_med INTEGER, db_rec_sq_alt INTEGER, db_rec_cq_rast INTEGER, db_rec_cq_med INTEGER, db_rec_cq_alt INTEGER, db_rec_cq_varr INTEGER, 
                    
                    -- DESVIOS (10)
                    db_desv_sq_pe INTEGER, db_desv_sq_mfr INTEGER, db_desv_sq_mlat INTEGER, db_desv_sq_a1 INTEGER, db_desv_sq_a2 INTEGER, db_desv_cq_varr INTEGER, db_desv_cq_r1 INTEGER, db_desv_cq_r2 INTEGER, db_desv_cq_a1 INTEGER, db_desv_cq_a2 INTEGER, 
                    
                    -- EXTENSÃO E VOO (7)
                    db_ext_rec INTEGER, db_ext_desv_1 INTEGER, db_ext_desv_2 INTEGER, db_voo_rec INTEGER, db_voo_desv_1 INTEGER, db_voo_desv_2 INTEGER, db_voo_desv_mc INTEGER, 
                    
                    -- CONTROLO DO ESPAÇO (4)
                    de_cabeca INTEGER, de_carrinho INTEGER, de_alivio INTEGER, de_rececao INTEGER, 
                    
                    -- DUELOS (4)
                    duelo_parede INTEGER, duelo_abafo INTEGER, duelo_estrela INTEGER, duelo_frontal INTEGER, 
                    
                    -- DISTRIBUIÇÃO (10)
                    pa_curto_1 INTEGER, pa_curto_2 INTEGER, pa_longo_1 INTEGER, pa_longo_2 INTEGER, dist_curta_mao INTEGER, dist_longa_mao INTEGER, dist_picada_mao INTEGER, dist_volley INTEGER, dist_curta_pe INTEGER, dist_longa_pe INTEGER, 
                    
                    -- CRUZAMENTOS (4)
                    cruz_rec_alta INTEGER, cruz_soco_1 INTEGER, cruz_soco_2 INTEGER, cruz_int_rast INTEGER,
                    
                    -- ESQUEMAS TÁTICOS (3)
                    eto_pb_curto INTEGER, eto_pb_medio INTEGER, eto_pb_longo INTEGER
                    )''')
    
    # 9. Tabela de Adversários (Scouting)
    c.execute('''CREATE TABLE IF NOT EXISTS opponents (
                    id INTEGER PRIMARY KEY, user_id TEXT, name TEXT, notes TEXT)''')
    
    # 10. Ficheiros de Adversários
    c.execute('''CREATE TABLE IF NOT EXISTS opponent_files (
                    id INTEGER PRIMARY KEY, opponent_id INTEGER, name TEXT, type TEXT, content BLOB, link TEXT)''')
    
    # 11. Pastas da Biblioteca
    c.execute('''CREATE TABLE IF NOT EXISTS library_folders (
                    id INTEGER PRIMARY KEY, user_id TEXT, name TEXT)''')
    
    # 12. Ficheiros da Biblioteca
    c.execute('''CREATE TABLE IF NOT EXISTS library_files (
                    id INTEGER PRIMARY KEY, folder_id INTEGER, name TEXT, type TEXT, content BLOB, link TEXT, description TEXT)''')

    # --- ATUALIZAÇÕES AUTOMÁTICAS (MIGRAÇÕES) ---
    try: c.execute("ALTER TABLE matches ADD COLUMN match_type TEXT")
    except: pass
    try: c.execute("ALTER TABLE sessions ADD COLUMN status TEXT")
    except: pass
    try: c.execute("ALTER TABLE sessions ADD COLUMN opponent TEXT")
    except: pass
    try: c.execute("ALTER TABLE sessions ADD COLUMN match_time TEXT")
    except: pass
    try: c.execute("ALTER TABLE sessions ADD COLUMN location TEXT")
    except: pass

    conn.commit()
    conn.close()

check_db_updates()

# ==========================================
# 3. FUNÇÕES AUXILIARES E PDF
# ==========================================
def parse_drills(drills_str):
    if not drills_str: return []
    try: return json.loads(drills_str)
    except:
        titles = drills_str.split(", ")
        return [{"title": t, "reps": "", "sets": "", "time": ""} for t in titles if t]

def safe_text(text):
    if not text: return ""
    try: return text.encode('latin-1', 'replace').decode('latin-1')
    except: return str(text)

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 16)
        self.cell(0, 10, 'GK MANAGER PRO - FICHA DE TREINO', 0, 1, 'C')
        self.ln(5)
    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

def create_training_pdf(user, session_info, athletes, drills_config, drills_details_df):
    pdf = PDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.set_fill_color(240, 240, 240)
    
    pdf.cell(0, 10, txt=safe_text(f"Treinador: {user}"), ln=1, align='L')
    pdf.cell(0, 10, txt=safe_text(f"Data: {session_info['start_date']} | Tipo: {session_info['type']}"), ln=1, align='L', fill=True)
    status_txt = session_info.get('status', 'Agendado')
    pdf.cell(0, 10, txt=safe_text(f"Foco: {session_info['title']} ({status_txt})"), ln=1, align='L')
    pdf.ln(5)
    
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(0, 10, safe_text("Lista de Presenças"), ln=1)
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(80, 10, safe_text("Nome do Atleta"), 1)
    pdf.cell(30, 10, safe_text("Presença"), 1)
    pdf.cell(30, 10, "Obs", 1)
    pdf.ln()
    pdf.set_font("Arial", size=10)
    
    if not athletes.empty:
        for _, row in athletes.iterrows():
            pdf.cell(80, 10, safe_text(f"{row['name']} ({row['status']})"), 1)
            pdf.cell(30, 10, "[   ]", 1)
            pdf.cell(30, 10, "", 1)
            pdf.ln()
    else: pdf.cell(0, 10, safe_text("Sem atletas registados"), 1, 1)
    pdf.ln(10)
    
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(0, 10, safe_text("Plano de Exercícios"), ln=1, align='C')
    pdf.ln(5)
    
    if drills_config:
        for i, config in enumerate(drills_config):
            title = config['title']
            details = drills_details_df[drills_details_df['title'] == title]
            
            if not details.empty:
                row = details.iloc[0]
                pdf.set_font("Arial", 'B', 14)
                pdf.set_fill_color(230, 230, 250)
                pdf.cell(0, 10, safe_text(f"Ex {i+1}: {title}"), 1, 1, 'L', fill=True)
                
                pdf.set_font("Arial", 'B', 10)
                pdf.set_fill_color(255, 255, 224) 
                load_text = f"Series: {config.get('sets','-')} | Reps: {config.get('reps','-')} | Tempo: {config.get('time','-')}"
                pdf.cell(0, 8, safe_text(load_text), 1, 1, 'L', fill=True)
                
                pdf.set_font("Arial", size=10)
                info_text = f"Momento: {row['moment']} | Tipo: {row['training_type']}"
                if row['space']: info_text += f" | Espaço: {row['space']}"
                pdf.write(5, safe_text(info_text))
                pdf.ln(6)
                
                if row['objective']: 
                    pdf.set_font("Arial", 'B', 10); pdf.write(5, "Obj: ")
                    pdf.set_font("Arial", '', 10); pdf.write(5, safe_text(f"{row['objective']}")); pdf.ln(6)
                if row['materials']: 
                    pdf.set_font("Arial", 'B', 10); pdf.write(5, "Mat: ")
                    pdf.set_font("Arial", '', 10); pdf.write(5, safe_text(f"{row['materials']}")); pdf.ln(6)
                pdf.ln(2)
                
                if row['image']:
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_img:
                            img = Image.open(io.BytesIO(row['image']))
                            img.save(temp_img.name)
                            pdf.image(temp_img.name, x=10, w=100)
                            pdf.ln(5)
                        os.unlink(temp_img.name)
                    except: pass
                
                pdf.set_font("Arial", 'B', 11)
                pdf.cell(0, 8, safe_text("Descrição:"), 0, 1)
                pdf.set_font("Arial", size=10)
                pdf.multi_cell(0, 6, safe_text(row['description']))
                pdf.ln(10)
                
                if pdf.get_y() > 240: pdf.add_page()
    else: pdf.cell(0, 10, safe_text("Sem exercícios planeados."), 0, 1)
    
    return pdf.output(dest='S').encode('latin-1')

# ==========================================
# 4. LOGIN
# ==========================================
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'username' not in st.session_state: st.session_state['username'] = ''

def login_page():
    st.title("🔐 GK Manager Pro")
    menu = ["Login", "Criar Conta"]
    choice = st.selectbox("Menu", menu)
    if choice == "Login":
        user = st.text_input("Utilizador")
        pwd = st.text_input("Password", type='password')
        if st.button("Entrar"):
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT * FROM users WHERE username=? AND password=?", (user, make_hashes(pwd)))
            if c.fetchall():
                st.session_state['logged_in'] = True
                st.session_state['username'] = user
                st.rerun()
            else: st.error("Erro no login")
    elif choice == "Criar Conta":
        new_u = st.text_input("Novo User")
        new_p = st.text_input("Nova Pass", type='password')
        if st.button("Registar"):
            conn = get_db_connection()
            try:
                conn.cursor().execute("INSERT INTO users VALUES (?,?)", (new_u, make_hashes(new_p)))
                conn.commit()
                st.success("Conta criada!")
                backup_to_drive() # Backup automático
            except: st.warning("Já existe.")
            conn.close()

# ==========================================
# 5. APLICAÇÃO PRINCIPAL
# ==========================================
def main_app():
    user = st.session_state['username']
    st.sidebar.title(f"👤 {user}")
    
    menu = st.sidebar.radio("Navegação", 
        ["Dashboard Geral", 
         "Gestão Semanal", 
         "Estatísticas & Presenças", 
         "Scouting & Adversários", 
         "Biblioteca de Documentos", 
         "Relatórios & Avaliações", 
         "Evolução do Atleta", 
         "Centro de Jogo", 
         "Calendário", 
         "Meus Atletas", 
         "Exercícios",
         "💾 Backups & Dados"])
    
    if st.sidebar.button("Sair"):
        backup_to_drive() # Backup automático
        st.session_state['logged_in'] = False
        st.rerun()

    # --- 0. DASHBOARD GERAL ---
    if menu == "Dashboard Geral":
        st.header("📊 Visão Geral da Época")
        conn = get_db_connection()
        n_treinos = conn.execute("SELECT count(*) FROM sessions WHERE user_id=? AND type='Treino' AND (status IS NULL OR status != 'Cancelado')", (user,)).fetchone()[0]
        n_descanso = conn.execute("SELECT count(*) FROM sessions WHERE user_id=? AND type='Descanso'", (user,)).fetchone()[0]
        jogos_oficiais = conn.execute("SELECT count(*) FROM matches WHERE user_id=? AND match_type='Oficial'", (user,)).fetchone()[0]
        jogos_amigaveis = conn.execute("SELECT count(*) FROM matches WHERE user_id=? AND match_type='Amigável'", (user,)).fetchone()[0]
        conn.close()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("⚽ Treinos", n_treinos)
        col2.metric("🏆 Oficiais", jogos_oficiais)
        col3.metric("🤝 Amigáveis", jogos_amigaveis)
        col4.metric("💤 Descansos", n_descanso)

    # --- 1. GESTÃO SEMANAL ---
    elif menu == "Gestão Semanal":
        st.header("📆 Planeamento Semanal")
        tab1, tab2 = st.tabs(["1. Criar Semana", "2. Planear Dias"])
        
        with tab1:
            with st.form("new_micro"):
                c1, c2 = st.columns(2)
                mt = c1.text_input("Nome da Semana")
                sd = c2.date_input("Início", datetime.today())
                mg = st.text_area("Objetivo")
                if st.form_submit_button("Criar Semana"):
                    conn = get_db_connection()
                    conn.cursor().execute("INSERT INTO microcycles (user_id, title, start_date, goal) VALUES (?,?,?,?)", (user, mt, sd, mg))
                    conn.commit(); conn.close(); 
                    backup_to_drive()
                    st.success("Criado!")
        
        with tab2:
            conn = get_db_connection()
            micros = pd.read_sql_query("SELECT * FROM microcycles WHERE user_id = ? ORDER BY start_date DESC", conn, params=(user,))
            conn.close()
            
            if not micros.empty:
                sel_micro = st.selectbox("Escolher Semana", micros['title'].unique())
                micro_data = micros[micros['title'] == sel_micro].iloc[0]
                base_date = datetime.strptime(micro_data['start_date'], '%Y-%m-%d')
                st.info(f"🎯 Objetivo: {micro_data['goal']}")
                
                for i in range(7):
                    curr = base_date + timedelta(days=i)
                    d_str = curr.strftime("%Y-%m-%d")
                    d_name = curr.strftime("%A")
                    
                    conn_d = get_db_connection()
                    sess = pd.read_sql_query("SELECT * FROM sessions WHERE user_id=? AND start_date=?", conn_d, params=(user, d_str))
                    conn_d.close()
                    
                    icon = "⚪"
                    header_extra = ""
                    if not sess.empty:
                        t = sess.iloc[0]['type']
                        stt = sess.iloc[0].get('status', 'Realizado')
                        if stt == 'Cancelado':
                            icon = "❌"
                            header_extra = "(Cancelado)"
                        elif t=="Treino": 
                            icon = "⚽"
                        elif t=="Jogo": 
                            icon = "🔴"
                            opp = sess.iloc[0].get('opponent', '')
                            m_time = sess.iloc[0].get('match_time', '')
                            loc = sess.iloc[0].get('location', '')
                            if opp: header_extra = f"- Jogo vs {opp} ({m_time} - {loc})"
                        elif t=="Descanso": 
                            icon = "🟢"
                    
                    with st.expander(f"{icon} {d_name} ({d_str}) {header_extra}"):
                        if not sess.empty and sess.iloc[0]['type'] == 'Treino' and sess.iloc[0].get('status') != 'Cancelado':
                            col_pdf, _ = st.columns([1,3])
                            with col_pdf:
                                s_data = sess.iloc[0]
                                drills_config = parse_drills(s_data['drills_list'])
                                drill_names = [d['title'] for d in drills_config]
                                if drill_names:
                                    ph = ','.join('?' for _ in drill_names)
                                    q = f"SELECT * FROM exercises WHERE user_id=? AND title IN ({ph})"
                                    p = [user] + drill_names
                                    conn_pdf = get_db_connection()
                                    d_df = pd.read_sql_query(q, conn_pdf, params=p)
                                    a_df = pd.read_sql_query("SELECT name, status FROM goalkeepers WHERE user_id=?", conn_pdf, params=(user,))
                                    conn_pdf.close()
                                    try:
                                        pdf_bytes = create_training_pdf(user, s_data, a_df, drills_config, d_df)
                                        st.download_button("📥 Baixar PDF", pdf_bytes, f"Treino_{d_str}.pdf", "application/pdf")
                                    except: st.error("Erro PDF")
                            
                            st.markdown("---")
                            conn_p = get_db_connection()
                            all_gks = pd.read_sql_query("SELECT id, name FROM goalkeepers WHERE user_id=?", conn_p, params=(user,))
                            sess_id = int(sess.iloc[0]['id'])
                            pres_exist = pd.read_sql_query("SELECT gk_id FROM attendance WHERE session_id=?", conn_p, params=(sess_id,))
                            conn_p.close()
                            current_present_ids = pres_exist['gk_id'].tolist() if not pres_exist.empty else []
                            current_present_names = all_gks[all_gks['id'].isin(current_present_ids)]['name'].tolist()
                            
                            with st.form(f"att_{d_str}"):
                                selected_gks = st.multiselect("Presentes:", all_gks['name'].tolist(), default=current_present_names)
                                if st.form_submit_button("Guardar Presenças"):
                                    ids_to_save = all_gks[all_gks['name'].isin(selected_gks)]['id'].tolist()
                                    conn_s = get_db_connection()
                                    c = conn_s.cursor()
                                    c.execute("DELETE FROM attendance WHERE session_id=?", (sess_id,))
                                    for gk_id in ids_to_save: c.execute("INSERT INTO attendance (session_id, gk_id, status) VALUES (?,?,?)", (sess_id, gk_id, 'Presente'))
                                    conn_s.commit(); conn_s.close()
                                    backup_to_drive()
                                    st.success("Atualizado!")
                            st.markdown("---")

                        with st.form(f"f_{d_str}"):
                            c_conf1, c_conf2 = st.columns(2)
                            prev_t = sess.iloc[0]['type'] if not sess.empty else "Treino"
                            prev_s = sess.iloc[0].get('status', 'Realizado') if not sess.empty else "Realizado"
                            if prev_s is None: prev_s = "Realizado"
                            
                            type_d = c_conf1.selectbox("Tipo", ["Treino", "Jogo", "Descanso"], index=["Treino", "Jogo", "Descanso"].index(prev_t), key=f"tp_{d_str}")
                            status_d = c_conf2.selectbox("Estado", ["Realizado", "Cancelado"], index=["Realizado", "Cancelado"].index(prev_s), key=f"st_{d_str}")
                            
                            opp_val, time_val, loc_val = "", time(15,0), "Casa"
                            if not sess.empty:
                                opp_val = sess.iloc[0].get('opponent', '')
                                t_str = sess.iloc[0].get('match_time', '15:00:00')
                                loc_val = sess.iloc[0].get('location', 'Casa')
                                try: time_val = datetime.strptime(t_str, '%H:%M:%S').time()
                                except: pass
                            
                            save_opp, save_time, save_loc = None, None, None
                            
                            if type_d == "Jogo":
                                st.info("🏆 Detalhes do Jogo")
                                c_j1, c_j2, c_j3 = st.columns(3)
                                save_opp = c_j1.text_input("Adversário", value=opp_val if opp_val else "", key=f"opp_{d_str}")
                                save_time = c_j2.time_input("Hora do Jogo", value=time_val, key=f"time_{d_str}")
                                save_loc = c_j3.radio("Local", ["Casa", "Fora"], index=0 if loc_val=="Casa" else 1, horizontal=True, key=f"loc_{d_str}")
                                sess_t = f"Jogo vs {save_opp}" 
                            elif type_d == "Treino":
                                def_t = sess.iloc[0]['title'] if not sess.empty else ""
                                sess_t = st.text_input("Foco / Tema", value=def_t, key=f"tit_{d_str}")
                            else:
                                sess_t = "Descanso"

                            new_config = []
                            drills_json = "[]"
                            
                            if type_d == "Treino":
                                current_config = parse_drills(sess.iloc[0]['drills_list']) if not sess.empty else []
                                current_titles = [d['title'] for d in current_config]
                                conn_ex = get_db_connection()
                                ddb = pd.read_sql_query("SELECT title, moment, training_type FROM exercises WHERE user_id=?", conn_ex, params=(user,))
                                conn_ex.close()
                                
                                # Filtro de Tipos
                                all_types = sorted(ddb['training_type'].unique().tolist()) if not ddb.empty else ["Técnico", "Tático"]
                                type_filter = st.multiselect("Filtrar Tipo", all_types, default=all_types, key=f"ft_{d_str}")
                                
                                moms = ["Defesa de Baliza", "Defesa do Espaço", "Cruzamento", "Duelos", "Distribuição", "Passe Atrasado"]
                                selected_in_tabs = []
                                drill_tabs = st.tabs(moms)
                                for k, mom in enumerate(moms):
                                    with drill_tabs[k]:
                                        if type_filter: options = ddb[(ddb['moment'] == mom) & (ddb['training_type'].isin(type_filter))]['title'].tolist()
                                        else: options = ddb[ddb['moment'] == mom]['title'].tolist()
                                        defaults = [t for t in current_titles if t in options]
                                        sel = st.multiselect(f"Exercícios ({mom})", options, default=defaults, key=f"ms_{d_str}_{mom}")
                                        selected_in_tabs.extend(sel)
                                if selected_in_tabs:
                                    st.markdown("###### Carga:")
                                    for i, title in enumerate(selected_in_tabs):
                                        old_vals = next((item for item in current_config if item["title"] == title), {'reps':'', 'sets':'', 'time':''})
                                        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
                                        with c1: st.markdown(f"**{title}**")
                                        with c2: r = st.text_input("Reps", value=old_vals.get('reps',''), key=f"r_{d_str}_{title}_{i}")
                                        with c3: s = st.text_input("Séries", value=old_vals.get('sets',''), key=f"s_{d_str}_{title}_{i}")
                                        with c4: t = st.text_input("Tempo", value=old_vals.get('time',''), key=f"tm_{d_str}_{title}_{i}")
                                        new_config.append({"title": title, "reps": r, "sets": s, "time": t})
                                drills_json = json.dumps(new_config)
                            
                            if st.form_submit_button("Guardar Planeamento"):
                                conn_s = get_db_connection()
                                c = conn_s.cursor()
                                chk = c.execute("SELECT id FROM sessions WHERE user_id=? AND start_date=?", (user, d_str)).fetchone()
                                s_time_str = save_time.strftime("%H:%M:%S") if save_time else None
                                
                                if chk: 
                                    c.execute("""UPDATE sessions SET type=?, title=?, drills_list=?, status=?, 
                                                 opponent=?, match_time=?, location=? WHERE id=?""", 
                                              (type_d, sess_t, drills_json, status_d, save_opp, s_time_str, save_loc, chk[0]))
                                else: 
                                    c.execute("""INSERT INTO sessions (user_id, type, title, start_date, drills_list, status, opponent, match_time, location) 
                                                 VALUES (?,?,?,?,?,?,?,?,?)""", 
                                              (user, type_d, sess_t, d_str, drills_json, status_d, save_opp, s_time_str, save_loc))
                                conn_s.commit(); conn_s.close()
                                backup_to_drive()
                                st.success("Guardado!"); st.rerun()
            else: st.warning("Cria uma semana primeiro.")

    # --- 2. ESTATÍSTICAS ---
    elif menu == "Estatísticas & Presenças":
        st.header("📈 Mapa de Presenças")
        col_d1, col_d2 = st.columns(2)
        start_filter = col_d1.date_input("De:", value=date.today() - timedelta(days=30))
        end_filter = col_d2.date_input("Até:", value=date.today())
        
        if start_filter <= end_filter:
            conn = get_db_connection()
            total_sessions = conn.execute("SELECT count(*) FROM sessions WHERE user_id=? AND type='Treino' AND (status IS NULL OR status != 'Cancelado') AND start_date >= ? AND start_date <= ?", (user, start_filter, end_filter)).fetchone()[0]
            gks = pd.read_sql_query("SELECT id, name FROM goalkeepers WHERE user_id=?", conn, params=(user,))
            att_data = []
            if total_sessions > 0:
                for _, gk in gks.iterrows():
                    n_p = conn.execute("""SELECT count(*) FROM attendance a JOIN sessions s ON a.session_id = s.id WHERE s.user_id=? AND a.gk_id=? AND s.type='Treino' AND (s.status IS NULL OR s.status != 'Cancelado') AND s.start_date >= ? AND s.start_date <= ?""", (user, gk['id'], start_filter, end_filter)).fetchone()[0]
                    att_data.append({"Nome": gk['name'], "Presenças": n_p, "Total": total_sessions, "%": f"{(n_p/total_sessions)*100:.1f}%"})
                df_att = pd.DataFrame(att_data)
                st.metric("Total Treinos", total_sessions)
                st.dataframe(df_att, use_container_width=True)
                if not df_att.empty:
                    df_att['Val'] = df_att['%'].str.replace('%','').astype(float)
                    st.bar_chart(df_att.set_index("Nome")['Val'])
            else: st.info("Sem treinos neste período.")
            conn.close()

    # --- 3. ESCOUTING ---
    elif menu == "Scouting & Adversários":
        st.header("🕵️ Scouting de Adversários")
        conn = get_db_connection()
        opps = pd.read_sql_query("SELECT * FROM opponents WHERE user_id=?", conn, params=(user,))
        conn.close()
        
        col_list, col_detail = st.columns([1, 2])
        
        with col_list:
            st.subheader("Equipas")
            with st.form("new_opp"):
                new_opp_name = st.text_input("Novo Adversário")
                if st.form_submit_button("➕ Criar"):
                    conn = get_db_connection()
                    conn.cursor().execute("INSERT INTO opponents (user_id, name) VALUES (?,?)", (user, new_opp_name))
                    conn.commit(); conn.close(); 
                    backup_to_drive()
                    st.success("Criado!"); st.rerun()
            
            if not opps.empty: selected_opp_name = st.radio("Selecionar:", opps['name'].tolist())
            else: selected_opp_name = None

        with col_detail:
            if selected_opp_name:
                opp_data = opps[opps['name'] == selected_opp_name].iloc[0]
                opp_id = int(opp_data['id'])
                
                st.subheader(f"Análise: {selected_opp_name}")
                
                with st.form("opp_notes"):
                    notes = st.text_area("Notas Táticas", value=opp_data['notes'] if opp_data['notes'] else "", height=150)
                    if st.form_submit_button("Guardar Notas"):
                        conn = get_db_connection()
                        conn.cursor().execute("UPDATE opponents SET notes=? WHERE id=?", (notes, opp_id))
                        conn.commit(); conn.close(); 
                        backup_to_drive()
                        st.success("Guardado"); st.rerun()
                
                st.markdown("---")
                st.write("### 📎 Anexos e Links")
                
                c_upl, c_lnk = st.tabs(["Carregar Ficheiro", "Adicionar Link"])
                with c_upl:
                    upl_file = st.file_uploader("Ficheiro (PDF, PPT, Imagem)", key="opp_upl")
                    if st.button("Guardar Ficheiro"):
                        if upl_file:
                            blob = upl_file.read()
                            conn = get_db_connection()
                            conn.cursor().execute("INSERT INTO opponent_files (opponent_id, name, type, content) VALUES (?,?,?,?)", (opp_id, upl_file.name, "file", blob))
                            conn.commit(); conn.close(); 
                            backup_to_drive()
                            st.success("Ficheiro anexado!"); st.rerun()
                with c_lnk:
                    lnk_url = st.text_input("URL (Youtube, Drive, etc)")
                    lnk_name = st.text_input("Nome do Link")
                    if st.button("Guardar Link"):
                        if lnk_url and lnk_name:
                            conn = get_db_connection()
                            conn.cursor().execute("INSERT INTO opponent_files (opponent_id, name, type, link) VALUES (?,?,?,?)", (opp_id, lnk_name, "link", lnk_url))
                            conn.commit(); conn.close(); 
                            backup_to_drive()
                            st.success("Link guardado!"); st.rerun()
                
                conn = get_db_connection()
                files = pd.read_sql_query("SELECT * FROM opponent_files WHERE opponent_id=?", conn, params=(opp_id,))
                conn.close()
                
                if not files.empty:
                    for _, f in files.iterrows():
                        c1, c2, c3 = st.columns([4, 1, 1])
                        with c1:
                            if f['type'] == 'link': st.markdown(f"🔗 [{f['name']}]({f['link']})")
                            else: st.write(f"📄 {f['name']}")
                        with c2:
                            if f['type'] == 'file':
                                st.download_button("📥", f['content'], file_name=f['name'], key=f"dl_{f['id']}")
                        with c3:
                            if st.button("🗑️", key=f"del_oppf_{f['id']}"):
                                conn = get_db_connection()
                                conn.cursor().execute("DELETE FROM opponent_files WHERE id=?", (f['id'],))
                                conn.commit(); conn.close(); 
                                backup_to_drive()
                                st.rerun()
            else:
                st.info("Seleciona ou cria um adversário.")

    # --- 4. BIBLIOTECA ---
    elif menu == "Biblioteca Documentos":
        st.header("📚 Biblioteca Digital")
        
        conn = get_db_connection()
        folders = pd.read_sql_query("SELECT * FROM library_folders WHERE user_id=?", conn, params=(user,))
        conn.close()
        
        c_nav, c_content = st.columns([1, 3])
        
        with c_nav:
            st.subheader("Pastas")
            with st.form("new_folder"):
                nf = st.text_input("Nova Pasta")
                if st.form_submit_button("Criar"):
                    conn = get_db_connection()
                    conn.cursor().execute("INSERT INTO library_folders (user_id, name) VALUES (?,?)", (user, nf))
                    conn.commit(); conn.close(); 
                    backup_to_drive()
                    st.rerun()
            
            if not folders.empty:
                sel_folder = st.radio("Navegar:", folders['name'].tolist())
            else: sel_folder = None
            
        with c_content:
            if sel_folder:
                folder_id = int(folders[folders['name'] == sel_folder].iloc[0]['id'])
                st.subheader(f"📂 {sel_folder}")
                
                with st.expander("➕ Adicionar Documento ou Link"):
                    tab_f, tab_l = st.tabs(["Ficheiro", "Link"])
                    with tab_f:
                        lf = st.file_uploader("Documento")
                        desc_f = st.text_input("Descrição (Opcional)", key="df")
                        if st.button("Carregar"):
                            if lf:
                                blob = lf.read()
                                conn = get_db_connection()
                                conn.cursor().execute("INSERT INTO library_files (folder_id, name, type, content, description) VALUES (?,?,?,?,?)", (folder_id, lf.name, "file", blob, desc_f))
                                conn.commit(); conn.close(); 
                                backup_to_drive()
                                st.success("Adicionado!"); st.rerun()
                    with tab_l:
                        ll = st.text_input("URL")
                        ln = st.text_input("Nome")
                        desc_l = st.text_input("Descrição", key="dl")
                        if st.button("Adicionar Link"):
                            if ll and ln:
                                conn = get_db_connection()
                                conn.cursor().execute("INSERT INTO library_files (folder_id, name, type, link, description) VALUES (?,?,?,?,?)", (folder_id, ln, "link", ll, desc_l))
                                conn.commit(); conn.close(); 
                                backup_to_drive()
                                st.success("Adicionado!"); st.rerun()
                
                conn = get_db_connection()
                lib_files = pd.read_sql_query("SELECT * FROM library_files WHERE folder_id=?", conn, params=(folder_id,))
                conn.close()
                
                if not lib_files.empty:
                    for _, lf in lib_files.iterrows():
                        with st.container(border=True):
                            lc1, lc2 = st.columns([5, 1])
                            with lc1:
                                if lf['type'] == 'link': 
                                    st.markdown(f"🔗 **[{lf['name']}]({lf['link']})**")
                                else: 
                                    st.markdown(f"📄 **{lf['name']}**")
                                if lf['description']: st.caption(lf['description'])
                            with lc2:
                                if lf['type'] == 'file':
                                    st.download_button("📥", lf['content'], file_name=lf['name'], key=f"lib_dl_{lf['id']}")
                                if st.button("🗑️", key=f"lib_del_{lf['id']}"):
                                    conn = get_db_connection()
                                    conn.cursor().execute("DELETE FROM library_files WHERE id=?", (lf['id'],))
                                    conn.commit(); conn.close(); 
                                    backup_to_drive()
                                    st.rerun()
                else:
                    st.info("Pasta vazia.")
            else:
                st.info("Cria e seleciona uma pasta para começar.")

    # --- 5. RELATÓRIOS ---
    elif menu == "Relatórios & Avaliações":
        st.header("📝 Notas Técnicas")
        tab_dia, tab_sem = st.tabs(["Diário", "Semanal"])
        with tab_dia:
            rep_date = st.date_input("Dia", datetime.today())
            d_str = rep_date.strftime("%Y-%m-%d")
            conn = get_db_connection()
            sess = pd.read_sql_query("SELECT * FROM sessions WHERE user_id=? AND start_date=?", conn, params=(user, d_str))
            if not sess.empty:
                s_data = sess.iloc[0]
                if s_data.get('status') == 'Cancelado': st.error("⚠️ Treino Cancelado")
                st.info(f"Treino: {s_data['title']}")
                with st.form("daily_rep"):
                    r_txt = st.text_area("Relatório", value=s_data['report'] if s_data['report'] else "")
                    if st.form_submit_button("Guardar"):
                        c = conn.cursor(); c.execute("UPDATE sessions SET report=? WHERE id=?", (r_txt, int(s_data['id']))); conn.commit(); 
                        backup_to_drive(); st.success("Guardado"); st.rerun()
            conn.close()

    # --- 6. EVOLUÇÃO ---
    elif menu == "Evolução do Atleta":
        st.header("📈 Evolução")
        conn = get_db_connection()
        gks = pd.read_sql_query("SELECT id, name FROM goalkeepers WHERE user_id=?", conn, params=(user,))
        conn.close()
        if not gks.empty:
            sel_gk = st.selectbox("Atleta", gks['name'].tolist())
            gid = int(gks[gks['name']==sel_gk].iloc[0]['id'])
            conn = get_db_connection()
            hist = pd.read_sql_query("SELECT date, rating, notes FROM training_ratings WHERE user_id=? AND gk_id=? ORDER BY date ASC", conn, params=(user, gid))
            conn.close()
            if not hist.empty:
                st.line_chart(hist.set_index("date")['rating'])
                st.dataframe(hist, use_container_width=True)
                st.metric("Média", f"{hist['rating'].mean():.1f}")
            else: st.info("Sem dados.")
        else: st.warning("Crie atletas.")

    # --- 7. CENTRO DE JOGO ---
    elif menu == "Centro de Jogo":
        st.header("🏟️ Ficha de Jogo")
        conn = get_db_connection()
        games = pd.read_sql_query("SELECT start_date, title FROM sessions WHERE user_id=? AND type='Jogo' ORDER BY start_date DESC", conn, params=(user,))
        gks = pd.read_sql_query("SELECT id, name FROM goalkeepers WHERE user_id=?", conn, params=(user,))
        conn.close()
        
        if not games.empty:
            game_opt = [f"{r['start_date']} | {r['title']}" for _, r in games.iterrows()]
            sel_game = st.selectbox("Jogo", game_opt)
            sel_date = sel_game.split(" | ")[0]
            st.markdown("---")
            
            with st.form("match_stats"):
                st.subheader("Informação Base")
                c_top1, c_top2 = st.columns(2)
                match_type = c_top1.selectbox("Tipo de Jogo", ["Oficial", "Amigável"])
                gk = c_top2.selectbox("Guarda-Redes Titular", gks['name'].tolist() if not gks.empty else [])
                
                c1, c2, c3, c4 = st.columns(4)
                res = c1.text_input("Resultado")
                gls = c2.number_input("Golos Sofridos", 0, 20)
                svs = c3.number_input("Defesas Realizadas", 0, 50)
                rt = c4.slider("Avaliação (1-10)", 1, 10, 5)

                # --- ESTATÍSTICAS COMPLETAS (72 VARS) ---
                with st.expander("🧱 1. DEFESA DE BALIZA: BLOQUEIOS"):
                    b1, b2 = st.columns(2)
                    with b1:
                        st.caption("Sem Queda")
                        bloq_sq_r = st.number_input("Rasteiro (SQ)", 0, 20, key="b1")
                        bloq_sq_m = st.number_input("Médio (SQ)", 0, 20, key="b2")
                        bloq_sq_a = st.number_input("Alto (SQ)", 0, 20, key="b3")
                    with b2:
                        st.caption("Com Queda")
                        bloq_cq_r = st.number_input("Rasteiro (CQ)", 0, 20, key="b4")
                        bloq_cq_m = st.number_input("Médio (CQ)", 0, 20, key="b5")
                        bloq_cq_a = st.number_input("Alto (CQ)", 0, 20, key="b6")

                with st.expander("👐 2. DEFESA DE BALIZA: RECEÇÕES"):
                    r1, r2 = st.columns(2)
                    with r1:
                        rec_sq_m = st.number_input("Médio (SQ)", 0, 20, key="r1")
                        rec_sq_a = st.number_input("Alto (SQ)", 0, 20, key="r2")
                    with r2:
                        rec_cq_r = st.number_input("Rasteiro (CQ)", 0, 20, key="r3")
                        rec_cq_m = st.number_input("Médio (CQ)", 0, 20, key="r4")
                        rec_cq_a = st.number_input("Alto (CQ)", 0, 20, key="r5")
                        rec_cq_v = st.number_input("Varrimento", 0, 20, key="r6")

                with st.expander("🧤 3. DEFESA DE BALIZA: DESVIOS"):
                    d1, d2 = st.columns(2)
                    with d1:
                        desv_sq_p = st.number_input("Pé", 0, 20, key="d1")
                        desv_sq_mf = st.number_input("Médio Frontal", 0, 20, key="d2")
                        desv_sq_ml = st.number_input("Médio Lateral", 0, 20, key="d3")
                        desv_sq_a1 = st.number_input("Alto 1 Mão", 0, 20, key="d4")
                        desv_sq_a2 = st.number_input("Alto 2 Mãos", 0, 20, key="d5")
                    with d2:
                        desv_cq_v = st.number_input("Varrimento", 0, 20, key="d6")
                        desv_cq_r1 = st.number_input("Rasteiro 1 Mão", 0, 20, key="d7")
                        desv_cq_r2 = st.number_input("Rasteiro 2 Mãos", 0, 20, key="d8")
                        desv_cq_a1 = st.number_input("Alto 1 Mão (CQ)", 0, 20, key="d9")
                        desv_cq_a2 = st.number_input("Alto 2 Mãos (CQ)", 0, 20, key="d10")

                with st.expander("✈️ 4. DEFESA DE BALIZA: EXTENSÃO E VOO"):
                    e1, e2 = st.columns(2)
                    with e1:
                        ext_rec = st.number_input("Ext. Receção", 0, 20, key="e1")
                        ext_d1 = st.number_input("Ext. Desvio 1", 0, 20, key="e2")
                        ext_d2 = st.number_input("Ext. Desvio 2", 0, 20, key="e3")
                    with e2:
                        voo_rec = st.number_input("Voo Receção", 0, 20, key="v1")
                        voo_d1 = st.number_input("Voo Desvio 1", 0, 20, key="v2")
                        voo_d2 = st.number_input("Voo Desvio 2", 0, 20, key="v3")
                        voo_dmc = st.number_input("Voo Mão Contrária", 0, 20, key="v4")

                with st.expander("🚀 5. DEFESA DO ESPAÇO"):
                    de_cab = st.number_input("Cabeceamento", 0, 20)
                    de_car = st.number_input("Carrinho", 0, 20)
                    de_ali = st.number_input("Alívio", 0, 20)
                    de_rec = st.number_input("Receção", 0, 20)

                with st.expander("⚔️ 6. DUELOS (1x1)"):
                    du_par = st.number_input("Parede", 0, 20)
                    du_aba = st.number_input("Abafo", 0, 20)
                    du_est = st.number_input("Estrela", 0, 20)
                    du_fro = st.number_input("Ataque Frontal", 0, 20)

                with st.expander("🎯 7. DISTRIBUIÇÃO"):
                    pa_c1 = st.number_input("Passe Curto 1T", 0, 50)
                    pa_c2 = st.number_input("Passe Curto 2T", 0, 50)
                    pa_l1 = st.number_input("Passe Longo 1T", 0, 50)
                    pa_l2 = st.number_input("Passe Longo 2T", 0, 50)
                    di_cm = st.number_input("Mão Curta", 0, 50)
                    di_lm = st.number_input("Mão Longa", 0, 50)
                    di_pm = st.number_input("Mão Picada", 0, 50)
                    di_vo = st.number_input("Volley", 0, 50)
                    di_cp = st.number_input("Pé Curta", 0, 50)
                    di_lp = st.number_input("Pé Longa", 0, 50)

                with st.expander("⚽ 8. ESQUEMAS TÁTICOS OFENSIVOS"):
                    eto_pb_curto = st.number_input("Pontapé Baliza Curto", 0, 50)
                    eto_pb_medio = st.number_input("Pontapé Baliza Meia Distância", 0, 50)
                    eto_pb_longo = st.number_input("Pontapé Baliza Longo", 0, 50)

                with st.expander("🥅 9. CRUZAMENTOS"):
                    cr_rec = st.number_input("Cruz. Receção", 0, 50)
                    cr_s1 = st.number_input("Cruz. Soco 1", 0, 50)
                    cr_s2 = st.number_input("Cruz. Soco 2", 0, 50)
                    cr_int = st.number_input("Cruz. Interceção", 0, 50)

                rep = st.text_area("Análise do Treinador")
                
                if st.form_submit_button("Guardar Ficha de Jogo"):
                    conn = get_db_connection()
                    gid = int(gks[gks['name']==gk].iloc[0]['id']) if not gks.empty else 0
                    c = conn.cursor()
                    
                    # Apagar anterior
                    c.execute("DELETE FROM matches WHERE user_id=? AND date=?", (user, sel_date))
                    
                    # 72 Variáveis - Lista Completa
                    vals = (
                        user, sel_date, sel_opp, gid, gls, svs, res, rep, rt,
                        bloq_sq_r, bloq_sq_m, bloq_sq_a, bloq_cq_r, bloq_cq_m, bloq_cq_a,
                        rec_sq_m, rec_sq_a, rec_cq_r, rec_cq_m, rec_cq_a, rec_cq_v,
                        desv_sq_p, desv_sq_mf, desv_sq_ml, desv_sq_a1, desv_sq_a2, 
                        desv_cq_v, desv_cq_r1, desv_cq_r2, desv_cq_a1, desv_cq_a2,
                        ext_rec, ext_d1, ext_d2, voo_rec, voo_d1, voo_d2, voo_dmc,
                        de_cab, de_car, de_ali, de_rec, 
                        du_par, du_aba, du_est, du_fro,
                        pa_c1, pa_c2, pa_l1, pa_l2, di_cm, di_lm, di_pm, di_vo, di_cp, di_lp,
                        cr_rec, cr_s1, cr_s2, cr_int,
                        eto_pb_curto, eto_pb_medio, eto_pb_longo
                    )
                    
                    # String de placeholders
                    placeholders = ",".join(["?"] * len(vals))
                    
                    # Inserção
                    c.execute(f'''INSERT INTO matches (
                        id, user_id, date, opponent, gk_id, goals_conceded, saves, result, report, rating,
                        db_bloq_sq_rast, db_bloq_sq_med, db_bloq_sq_alt, db_bloq_cq_rast, db_bloq_cq_med, db_bloq_cq_alt,
                        db_rec_sq_med, db_rec_sq_alt, db_rec_cq_rast, db_rec_cq_med, db_rec_cq_alt, db_rec_cq_varr,
                        db_desv_sq_pe, db_desv_sq_mfr, db_desv_sq_mlat, db_desv_sq_a1, db_desv_sq_a2, db_desv_cq_varr, db_desv_cq_r1, db_desv_cq_r2, db_desv_cq_a1, db_desv_cq_a2,
                        db_ext_rec, db_ext_desv_1, db_ext_desv_2, db_voo_rec, db_voo_desv_1, db_voo_desv_2, db_voo_desv_mc,
                        de_cabeca, de_carrinho, de_alivio, de_rececao,
                        duelo_parede, duelo_abafo, duelo_estrela, duelo_frontal,
                        pa_curto_1, pa_curto_2, pa_longo_1, pa_longo_2, dist_curta_mao, dist_longa_mao, dist_picada_mao, dist_volley, dist_curta_pe, dist_longa_pe,
                        cruz_rec_alta, cruz_soco_1, cruz_soco_2, cruz_int_rast,
                        eto_pb_curto, eto_pb_medio, eto_pb_longo
                    ) VALUES (NULL, {placeholders})''', vals)
                    
                    # Atualizar tipo de jogo
                    c.execute("UPDATE matches SET match_type=? WHERE user_id=? AND date=?", (match_type, user, sel_date))
                    
                    conn.commit(); conn.close()
                    backup_to_drive()
                    st.success("Ficha de Jogo Completa Guardada!")
                    st.rerun()
            
            st.markdown("---")
            st.subheader("Histórico de Jogos")
            conn = get_db_connection()
            try:
                hist = pd.read_sql_query("SELECT date, match_type, opponent, result, rating FROM matches WHERE user_id=? ORDER BY date DESC", conn, params=(user,))
            except:
                hist = pd.read_sql_query("SELECT date, opponent, result, rating FROM matches WHERE user_id=? ORDER BY date DESC", conn, params=(user,))
            conn.close()
            if not hist.empty:
                st.dataframe(hist, use_container_width=True)
            else:
                st.info("Ainda sem jogos.")

        else: st.info("Marca jogos primeiro na Gestão Semanal.")

    # --- 8. CALENDÁRIO ---
    elif menu == "Calendário":
        st.header("📅 Calendário")
        conn = get_db_connection()
        sess = pd.read_sql_query("SELECT type, title, start_date, status, opponent, location FROM sessions WHERE user_id=?", conn, params=(user,))
        conn.close()
        evs = []
        for _, r in sess.iterrows():
            c = "#3788d8"
            title_display = r['title']
            if r.get('status')=='Cancelado': 
                c = "#6c757d"
                title_display += " (Cancelado)"
            elif r['type']=="Jogo": 
                c = "#d9534f"
                opp = r.get('opponent')
                loc = r.get('location')
                if opp: 
                    loc_short = "(C)" if loc == "Casa" else "(F)"
                    title_display = f"Jogo vs {opp} {loc_short}"
            elif r['type']=="Descanso": 
                c = "#28a745"
            evs.append({"title": title_display, "start": r['start_date'], "end": r['start_date'], "backgroundColor": c})
        calendar(events=evs, options={"initialView": "dayGridMonth"})

    # --- 9. ATLETAS ---
    elif menu == "Meus Atletas":
        st.header("📋 Plantel")
        mode = st.radio("Opções", ["Novo", "Editar", "Eliminar"], horizontal=True)
        conn = get_db_connection()
        all_gks = pd.read_sql_query("SELECT * FROM goalkeepers WHERE user_id=?", conn, params=(user,))
        conn.close()
        d_n, d_a, d_s = "", 18, "Apto"
        d_h, d_w, d_al, d_ar, d_gl = 0.0, 0.0, 0.0, 0.0, ""
        d_jf2, d_jfl, d_jfr, d_jll, d_jlr = 0.0, 0.0, 0.0, 0.0, 0.0
        d_tr, d_ta, d_tv = "", "", ""
        e_id = None
        if mode in ["Editar", "Eliminar"] and not all_gks.empty:
            s_gk = st.selectbox("Atleta", all_gks['name'].tolist())
            gk_d = all_gks[all_gks['name']==s_gk].iloc[0]
            e_id = int(gk_d['id'])
            d_n, d_a, d_s = gk_d['name'], int(gk_d['age']), gk_d['status']
            d_h, d_w, d_al, d_ar, d_gl = gk_d['height'], gk_d['wingspan'], gk_d['arm_len_left'], gk_d['arm_len_right'], gk_d['glove_size']
            d_jf2, d_jfl, d_jfr = gk_d['jump_front_2'], gk_d['jump_front_l'], gk_d['jump_front_r']
            d_jll, d_jlr = gk_d['jump_lat_l'], gk_d['jump_lat_r']
            d_tr, d_ta, d_tv = gk_d['test_res'], gk_d['test_agil'], gk_d['test_vel']
        
        if mode=="Eliminar" and e_id:
            if st.button("🗑️ Eliminar"):
                conn = get_db_connection(); conn.cursor().execute("DELETE FROM goalkeepers WHERE id=?", (e_id,)); conn.commit(); conn.close(); 
                backup_to_drive(); st.success("Apagado"); st.rerun()
        elif mode!="Eliminar":
            with st.form("gk_form"):
                st.subheader("1. Perfil")
                c1,c2,c3 = st.columns(3)
                nm = st.text_input("Nome", value=d_n)
                ag = st.number_input("Idade", value=d_a)
                stt = st.selectbox("Estado", ["Apto", "Lesionado"], index=0 if d_s=="Apto" else 1)
                st.subheader("2. Biometria")
                b1,b2,b3,b4,b5=st.columns(5)
                ht=b1.number_input("Altura", 0.0, 250.0, value=d_h)
                ws=b2.number_input("Envergadura", 0.0, 250.0, value=d_w)
                al=b3.number_input("Braço Esquerdo", 0.0, 150.0, value=d_al)
                ar=b4.number_input("Braço Direito", 0.0, 150.0, value=d_ar)
                gl=b5.text_input("Luva", value=d_gl)
                st.subheader("3. Saltos")
                j1,j2,j3=st.columns(3)
                jf2=j1.number_input("Frontal 2 Pés", 0.0, value=d_jf2)
                jfl=j2.number_input("Frontal Pé Esquerdo", 0.0, value=d_jfl)
                jfr=j3.number_input("Frontal Pé Direito", 0.0, value=d_jfr)
                j4,j5=st.columns(2)
                jll=j4.number_input("Lateral Esquerdo", 0.0, value=d_jll)
                jlr=j5.number_input("Lateral Direito", 0.0, value=d_jlr)
                st.subheader("4. Testes")
                t1,t2,t3=st.columns(3)
                tr=t1.text_input("Resistência", value=d_tr)
                ta=t2.text_input("Agilidade", value=d_ta)
                tv=t3.text_input("Velocidade", value=d_tv)
                if st.form_submit_button("Guardar"):
                    conn = get_db_connection(); c = conn.cursor()
                    if mode=="Novo":
                        c.execute('''INSERT INTO goalkeepers (user_id, name, age, status, height, wingspan, arm_len_left, arm_len_right, glove_size, jump_front_2, jump_front_l, jump_front_r, jump_lat_l, jump_lat_r, test_res, test_agil, test_vel) VALUES (?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?)''', 
                                  (user, nm, ag, stt, ht, ws, al, ar, gl, jf2, jfl, jfr, jll, jlr, tr, ta, tv))
                    elif e_id:
                        c.execute('''UPDATE goalkeepers SET name=?, age=?, status=?, height=?, wingspan=?, arm_len_left=?, arm_len_right=?, glove_size=?, jump_front_2=?, jump_front_l=?, jump_front_r=?, jump_lat_l=?, jump_lat_r=?, test_res=?, test_agil=?, test_vel=? WHERE id=?''', 
                                  (nm, ag, stt, ht, ws, al, ar, gl, jf2, jfl, jfr, jll, jlr, tr, ta, tv, e_id))
                    conn.commit(); conn.close(); 
                    backup_to_drive()
                    st.success("Guardado"); st.rerun()
        if not all_gks.empty: st.dataframe(all_gks.drop(columns=['user_id', 'notes']), use_container_width=True)

    # --- 10. EXERCÍCIOS ---
    elif menu == "Exercícios":
        st.header("Biblioteca Técnica")
        if 'edit_drill_id' not in st.session_state: st.session_state['edit_drill_id'] = None
        conn = get_db_connection()
        all_ex = pd.read_sql_query("SELECT * FROM exercises WHERE user_id=?", conn, params=(user,))
        conn.close()
        d_tit, d_mom, d_typ = "", "Defesa de Baliza", "Técnico"
        if st.session_state['edit_drill_id'] and not all_ex.empty:
            edit_row = all_ex[all_ex['id'] == st.session_state['edit_drill_id']].iloc[0]
            d_tit = edit_row['title']; d_mom = edit_row['moment']
            st.warning(f"✏️ A Editar: {d_tit}")
            if st.button("❌ Cancelar"): st.session_state['edit_drill_id'] = None; st.rerun()

        with st.form("drill_form"):
            st.subheader("Novo Exercício")
            title = st.text_input("Título", value=d_tit)
            moms = ["Defesa de Baliza", "Defesa do Espaço", "Cruzamento", "Duelos", "Distribuição", "Passe Atrasado"]
            typs = ["Técnico", "Tático", "Técnico-Tático", "Físico", "Psicológico"]
            c1, c2 = st.columns(2)
            moment = c1.selectbox("Momento", moms, index=moms.index(d_mom) if d_mom in moms else 0)
            train_type = c2.selectbox("Tipo", typs)
            space = st.text_input("Espaço")
            c3, c4 = st.columns(2)
            objective = c3.text_input("Objetivo")
            materials = c4.text_area("Material", height=100)
            desc = st.text_area("Descrição")
            img = st.file_uploader("Imagem do Exercício (Upload)", type=['png','jpg'])
            if st.form_submit_button("Guardar"):
                b_img = img.read() if img else None
                conn = get_db_connection(); c = conn.cursor()
                if not st.session_state['edit_drill_id']:
                    c.execute('''INSERT INTO exercises (user_id, title, moment, training_type, description, objective, materials, space, image) VALUES (?,?,?,?,?,?,?,?,?)''', (user, title, moment, train_type, desc, objective, materials, space, b_img))
                else:
                    eid = st.session_state['edit_drill_id']
                    if b_img: c.execute('''UPDATE exercises SET title=?, moment=?, training_type=?, description=?, objective=?, materials=?, space=?, image=? WHERE id=?''', (title, moment, train_type, desc, objective, materials, space, b_img, eid))
                    else: c.execute('''UPDATE exercises SET title=?, moment=?, training_type=?, description=?, objective=?, materials=?, space=? WHERE id=?''', (title, moment, train_type, desc, objective, materials, space, eid))
                    st.session_state['edit_drill_id'] = None
                conn.commit(); conn.close()
                backup_to_drive()
                st.success("Guardado!"); st.rerun()

        st.markdown("---")
        tabs = st.tabs(moms)
        if not all_ex.empty:
            for i, mom in enumerate(moms):
                with tabs[i]:
                    filt = all_ex[all_ex['moment'] == mom]
                    if not filt.empty:
                        for _, r in filt.iterrows():
                            with st.expander(f"[{r['training_type']}] {r['title']}"):
                                c_act, c_img, c_txt = st.columns([1, 2, 4])
                                with c_act:
                                    if st.button("✏️", key=f"ed_{r['id']}"): st.session_state['edit_drill_id'] = r['id']; st.rerun()
                                    if st.button("🗑️", key=f"dl_{r['id']}"):
                                        conn = get_db_connection(); conn.cursor().execute("DELETE FROM exercises WHERE id=?", (r['id'],)); conn.commit(); conn.close(); 
                                        backup_to_drive(); st.rerun()
                                with c_txt: st.caption(r['description'])
                                with c_img: 
                                    if r['image']: st.image(r['image'])
                    else: st.info("Vazio.")

    # --- 11. BACKUPS & DADOS (V44 - DRIVE + MANUAL) ---
    elif menu == "💾 Backups & Dados":
        st.header("💾 Centro de Recuperação e Segurança")
        st.info("O sistema tenta sincronizar automaticamente com o Google Drive ao guardar dados.")
        
        tab_drive, tab_down, tab_up = st.tabs(["☁️ Estado do Drive", "⬇️ Download PC", "⬆️ Restaurar Manual"])
        
        with tab_drive:
            st.write("Forçar sincronização manual com o Google Drive:")
            if st.button("📤 Enviar Backup para o Drive Agora"):
                with st.spinner("A enviar..."):
                    backup_to_drive()
            
            if st.button("📥 Baixar Backup do Drive (Substitui Local)"):
                with st.spinner("A baixar..."):
                    sync_download_db()
                    st.success("Sincronizado! A reiniciar..."); st.rerun()

        with tab_down:
            st.write("Guardar cópia local no PC:")
            if os.path.exists(DB_FILE):
                with open(DB_FILE, "rb") as fp:
                    st.download_button(
                        label="📥 Download Base de Dados (.db)",
                        data=fp,
                        file_name=f"backup_gk_manager_{datetime.now().strftime('%Y%m%d_%H%M')}.db",
                        mime="application/x-sqlite3"
                    )
            else: st.error("Base de dados não encontrada.")
        
        with tab_up:
            st.write("Restaurar dados antigos (Substitui os atuais):")
            uploaded_db = st.file_uploader("Carregar ficheiro .db", type=['db'])
            if uploaded_db is not None:
                if st.button("⚠️ Confirmar Restauro"):
                    with open(DB_FILE, "wb") as f: f.write(uploaded_db.getbuffer())
                    st.success("Restaurado! A reiniciar..."); st.rerun()

if st.session_state['logged_in']:
    main_app()
else:
    login_page()