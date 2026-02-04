import streamlit as st
import sqlite3
import pandas as pd
import hashlib
import json
# CORREÇÃO 1: Importação de timedelta adicionada para resolver o NameError
from datetime import datetime, timedelta
from streamlit_calendar import calendar
from fpdf import FPDF
import tempfile
import os
from PIL import Image
import io

# ==========================================
# 1. CONFIGURAÇÃO E BASE DE DADOS (V35)
# ==========================================
st.set_page_config(page_title="GK Manager Pro v35", layout="wide", page_icon="🧤")

def get_db_connection():
    # V35: Base de dados nova para garantir estrutura limpa
    conn = sqlite3.connect('gk_master_v35.db') 
    return conn

def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # USERS
    c.execute('''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)''')
    
    # ATLETAS
    c.execute('''CREATE TABLE IF NOT EXISTS goalkeepers (
                    id INTEGER PRIMARY KEY, user_id TEXT, name TEXT, age INTEGER, status TEXT, notes TEXT,
                    height REAL, wingspan REAL, arm_len_left REAL, arm_len_right REAL, glove_size TEXT,
                    jump_front_2 REAL, jump_front_l REAL, jump_front_r REAL, jump_lat_l REAL, jump_lat_r REAL,
                    test_res TEXT, test_agil TEXT, test_vel TEXT)''')
    
    # EXERCÍCIOS
    c.execute('''CREATE TABLE IF NOT EXISTS exercises (
                    id INTEGER PRIMARY KEY, user_id TEXT, title TEXT, moment TEXT, training_type TEXT, 
                    description TEXT, objective TEXT, materials TEXT, space TEXT, image BLOB)''')
    
    # SESSÕES
    c.execute('''CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY, user_id TEXT, type TEXT, title TEXT, start_date TEXT, drills_list TEXT, report TEXT)''')
    
    # MICROCICLOS
    c.execute('''CREATE TABLE IF NOT EXISTS microcycles (
                    id INTEGER PRIMARY KEY, user_id TEXT, title TEXT, start_date TEXT, goal TEXT, report TEXT)''')
    
    # NOTAS
    c.execute('''CREATE TABLE IF NOT EXISTS training_ratings (
                    id INTEGER PRIMARY KEY, user_id TEXT, date TEXT, gk_id INTEGER, rating INTEGER, notes TEXT)''')
    
    # JOGOS - ESTRUTURA BLINDADA (63 CAMPOS DE DADOS + ID)
    # CORREÇÃO 2: Garantir que a tabela tem exatamente as colunas que vamos inserir
    c.execute('''CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY, 
                    user_id TEXT, date TEXT, opponent TEXT, gk_id INTEGER, goals_conceded INTEGER, saves INTEGER, result TEXT, report TEXT, rating INTEGER, 
                    -- Bloqueios (6)
                    db_bloq_sq_rast INTEGER, db_bloq_sq_med INTEGER, db_bloq_sq_alt INTEGER, db_bloq_cq_rast INTEGER, db_bloq_cq_med INTEGER, db_bloq_cq_alt INTEGER, 
                    -- Rececoes (6)
                    db_rec_sq_med INTEGER, db_rec_sq_alt INTEGER, db_rec_cq_rast INTEGER, db_rec_cq_med INTEGER, db_rec_cq_alt INTEGER, db_rec_cq_varr INTEGER, 
                    -- Desvios (10)
                    db_desv_sq_pe INTEGER, db_desv_sq_mfr INTEGER, db_desv_sq_mlat INTEGER, db_desv_sq_a1 INTEGER, db_desv_sq_a2 INTEGER, db_desv_cq_varr INTEGER, db_desv_cq_r1 INTEGER, db_desv_cq_r2 INTEGER, db_desv_cq_a1 INTEGER, db_desv_cq_a2 INTEGER, 
                    -- Ext/Voo (7)
                    db_ext_rec INTEGER, db_ext_desv_1 INTEGER, db_ext_desv_2 INTEGER, db_voo_rec INTEGER, db_voo_desv_1 INTEGER, db_voo_desv_2 INTEGER, db_voo_desv_mc INTEGER, 
                    -- Espaco (4)
                    de_cabeca INTEGER, de_carrinho INTEGER, de_alivio INTEGER, de_rececao INTEGER, 
                    -- Duelos (4)
                    duelo_parede INTEGER, duelo_abafo INTEGER, duelo_estrela INTEGER, duelo_frontal INTEGER, 
                    -- Tactica (10)
                    pa_curto_1 INTEGER, pa_curto_2 INTEGER, pa_longo_1 INTEGER, pa_longo_2 INTEGER, dist_curta_mao INTEGER, dist_longa_mao INTEGER, dist_picada_mao INTEGER, dist_volley INTEGER, dist_curta_pe INTEGER, dist_longa_pe INTEGER, 
                    -- Cruzamentos (4)
                    cruz_rec_alta INTEGER, cruz_soco_1 INTEGER, cruz_soco_2 INTEGER, cruz_int_rast INTEGER,
                    -- ETO (3)
                    eto_pb_curto INTEGER, eto_pb_medio INTEGER, eto_pb_longo INTEGER
                    )''')
    conn.commit()
    conn.close()

init_db()

# ==========================================
# 2. FUNÇÕES AUXILIARES
# ==========================================
def parse_drills(drills_str):
    if not drills_str: return []
    try: return json.loads(drills_str)
    except:
        titles = drills_str.split(", ")
        return [{"title": t, "reps": "", "sets": "", "time": ""} for t in titles if t]

class PDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 16)
        self.cell(0, 10, 'GK MANAGER PRO - FICHA DE TREINO', 0, 1, 'C')
        self.ln(5)
    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

def create_training_pdf(user, session_info, athletes, drills_config, drills_details_df):
    pdf = PDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(0, 10, txt=f"Treinador: {user}", ln=1, align='L')
    pdf.cell(0, 10, txt=f"Data: {session_info['start_date']} | Tipo: {session_info['type']}", ln=1, align='L', fill=True)
    pdf.cell(0, 10, txt=f"Foco Principal: {session_info['title']}", ln=1, align='L')
    pdf.ln(5)
    
    pdf.set_font("Helvetica", 'B', 14)
    pdf.cell(0, 10, "Lista de Presencas", ln=1)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(80, 10, "Nome do Atleta", 1)
    pdf.cell(30, 10, "Presenca", 1)
    pdf.cell(30, 10, "Obs", 1)
    pdf.ln()
    pdf.set_font("Helvetica", size=10)
    if not athletes.empty:
        for _, row in athletes.iterrows():
            pdf.cell(80, 10, f"{row['name']} ({row['status']})", 1)
            pdf.cell(30, 10, "[   ]", 1)
            pdf.cell(30, 10, "", 1)
            pdf.ln()
    else: pdf.cell(0, 10, "Sem atletas registados", 1, 1)
    pdf.ln(10)
    
    pdf.add_page()
    pdf.set_font("Helvetica", 'B', 16)
    pdf.cell(0, 10, "Plano de Exercicios", ln=1, align='C')
    pdf.ln(5)
    
    if drills_config:
        for i, config in enumerate(drills_config):
            title = config['title']
            details = drills_details_df[drills_details_df['title'] == title]
            if not details.empty:
                row = details.iloc[0]
                pdf.set_font("Helvetica", 'B', 14)
                pdf.set_fill_color(230, 230, 250)
                pdf.cell(0, 10, f"Ex {i+1}: {title}", 1, 1, 'L', fill=True)
                
                pdf.set_font("Helvetica", 'B', 10)
                pdf.set_fill_color(255, 255, 224) 
                load_text = f"Series: {config.get('sets','-')} | Repeticoes: {config.get('reps','-')} | Tempo: {config.get('time','-')}"
                pdf.cell(0, 8, load_text, 1, 1, 'L', fill=True)
                
                pdf.set_font("Helvetica", size=10)
                pdf.write(5, f"Momento: {row['moment']} | Tipo: {row['training_type']}")
                if row['space']: pdf.write(5, f" | Espaco: {row['space']}")
                pdf.ln(6)
                if row['objective']: 
                    pdf.set_font("Helvetica", 'B', 10); pdf.write(5, "Objetivo: ")
                    pdf.set_font("Helvetica", '', 10); pdf.write(5, f"{row['objective']}"); pdf.ln(6)
                if row['materials']: 
                    pdf.set_font("Helvetica", 'B', 10); pdf.write(5, "Material: ")
                    pdf.set_font("Helvetica", '', 10); pdf.write(5, f"{row['materials']}"); pdf.ln(6)
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
                pdf.set_font("Helvetica", 'B', 11)
                pdf.cell(0, 8, "Descricao / Processo:", 0, 1)
                pdf.set_font("Helvetica", size=10)
                pdf.multi_cell(0, 6, row['description'])
                pdf.ln(10)
                if pdf.get_y() > 240: pdf.add_page()
    else: pdf.cell(0, 10, "Sem exercicios.", 0, 1)
    
    # CORREÇÃO 3: Retornar bytes diretamente (sem encode) para evitar erro do PDF
    return bytes(pdf.output())

# ==========================================
# 3. SISTEMA DE LOGIN
# ==========================================
if 'logged_in' not in st.session_state: st.session_state['logged_in'] = False
if 'username' not in st.session_state: st.session_state['username'] = ''

def login_page():
    st.title("🔐 GK Manager v35")
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
            except: st.warning("Já existe.")
            conn.close()

# ==========================================
# 4. APLICAÇÃO PRINCIPAL
# ==========================================
def main_app():
    user = st.session_state['username']
    st.sidebar.title(f"👤 {user}")
    menu = st.sidebar.radio("Navegação", 
        ["Gestão Semanal", "Relatórios & Avaliações", "Evolução do Atleta", "Centro de Jogo", "Calendário", "Meus Atletas", "Exercícios"])
    
    if st.sidebar.button("Sair"):
        st.session_state['logged_in'] = False
        st.rerun()

    # --- 1. GESTÃO SEMANAL ---
    if menu == "Gestão Semanal":
        st.header("📆 Planeamento")
        tab1, tab2 = st.tabs(["1. Criar Semana", "2. Planear Dias"])
        with tab1:
            with st.form("new_micro"):
                c1, c2 = st.columns(2)
                mt = c1.text_input("Nome da Semana")
                sd = c2.date_input("Início", datetime.today())
                mg = st.text_area("Objetivo")
                if st.form_submit_button("Criar Semana"):
                    conn = get_db_connection()
                    c = conn.cursor()
                    c.execute("INSERT INTO microcycles (user_id, title, start_date, goal) VALUES (?,?,?,?)", (user, mt, sd, mg))
                    conn.commit()
                    conn.close()
                    st.success("Criado!")
        with tab2:
            conn = get_db_connection()
            micros = pd.read_sql_query("SELECT * FROM microcycles WHERE user_id = ? ORDER BY start_date DESC", conn, params=(user,))
            conn.close()
            if not micros.empty:
                sel_micro = st.selectbox("Escolher Semana", micros['title'].unique())
                micro_data = micros[micros['title'] == sel_micro].iloc[0]
                base_date = datetime.strptime(micro_data['start_date'], '%Y-%m-%d')
                st.info(f"Objetivo: {micro_data['goal']}")
                
                for i in range(7):
                    curr = base_date + timedelta(days=i)
                    d_str = curr.strftime("%Y-%m-%d")
                    d_name = curr.strftime("%A")
                    
                    conn_d = get_db_connection()
                    sess = pd.read_sql_query("SELECT * FROM sessions WHERE user_id=? AND start_date=?", conn_d, params=(user, d_str))
                    conn_d.close()
                    
                    icon = "⚪"
                    if not sess.empty:
                        t = sess.iloc[0]['type']
                        icon = "⚽" if t=="Treino" else ("🔴" if t=="Jogo" else "🟢")
                    
                    with st.expander(f"{icon} {d_name} ({d_str})"):
                        if not sess.empty and sess.iloc[0]['type'] == 'Treino':
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
                                        pdf_data = create_training_pdf(user, s_data, a_df, drills_config, d_df)
                                        st.download_button("📄 PDF do Treino", pdf_data, f"Treino_{d_str}.pdf", "application/pdf")
                                    except Exception as e: st.error(f"Erro PDF: {e}")

                        with st.form(f"f_{d_str}"):
                            prev_t = sess.iloc[0]['type'] if not sess.empty else "Treino"
                            opts = ["Treino", "Jogo", "Descanso"]
                            idx = opts.index(prev_t) if prev_t in opts else 0
                            type_d = st.radio("Tipo", opts, index=idx, horizontal=True, key=f"rd_{d_str}")
                            
                            def_t = sess.iloc[0]['title'] if not sess.empty else ""
                            current_config = parse_drills(sess.iloc[0]['drills_list']) if not sess.empty else []
                            current_titles = [d['title'] for d in current_config]
                            sess_t = st.text_input("Foco", value=def_t, key=f"tit_{d_str}")
                            
                            conn_ex = get_db_connection()
                            ddb = pd.read_sql_query("SELECT title, moment FROM exercises WHERE user_id=?", conn_ex, params=(user,))
                            conn_ex.close()
                            
                            st.write("---")
                            st.caption("Selecionar Exercícios por Momento:")
                            moms = ["Defesa de Baliza", "Defesa do Espaço", "Cruzamento", "Duelos", "Distribuição", "Passe Atrasado"]
                            selected_in_tabs = []
                            drill_tabs = st.tabs(moms)
                            for k, mom in enumerate(moms):
                                with drill_tabs[k]:
                                    options = ddb[ddb['moment'] == mom]['title'].tolist()
                                    defaults = [t for t in current_titles if t in options]
                                    sel = st.multiselect(f"Exercícios ({mom})", options, default=defaults, key=f"ms_{d_str}_{mom}")
                                    selected_in_tabs.extend(sel)
                            
                            if selected_in_tabs:
                                st.markdown("###### Carga:")
                                new_config = []
                                for title in selected_in_tabs:
                                    old_vals = next((item for item in current_config if item["title"] == title), {'reps':'', 'sets':'', 'time':''})
                                    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
                                    with c1: st.markdown(f"**{title}**")
                                    with c2: r = st.text_input("Reps", value=old_vals.get('reps',''), key=f"r_{d_str}_{title}")
                                    with c3: s = st.text_input("Séries", value=old_vals.get('sets',''), key=f"s_{d_str}_{title}")
                                    with c4: t = st.text_input("Tempo", value=old_vals.get('time',''), key=f"tm_{d_str}_{title}")
                                    new_config.append({"title": title, "reps": r, "sets": s, "time": t})
                            else: new_config = []
                            
                            if st.form_submit_button("Guardar Planeamento"):
                                drills_json = json.dumps(new_config)
                                conn_s = get_db_connection()
                                c = conn_s.cursor()
                                chk = c.execute("SELECT id FROM sessions WHERE user_id=? AND start_date=?", (user, d_str)).fetchone()
                                if chk: c.execute("UPDATE sessions SET type=?, title=?, drills_list=? WHERE id=?", (type_d, sess_t, drills_json, chk[0]))
                                else: c.execute("INSERT INTO sessions (user_id, type, title, start_date, drills_list) VALUES (?,?,?,?,?)", (user, type_d, sess_t, d_str, drills_json))
                                conn_s.commit()
                                conn_s.close()
                                st.success("Guardado")
                                st.rerun()
            else: st.warning("Cria uma semana.")

    # --- 2. RELATÓRIOS ---
    elif menu == "Relatórios & Avaliações":
        st.header("📝 Relatório e Notas")
        tab_dia, tab_sem = st.tabs(["Relatório Diário", "Relatório Semanal"])
        with tab_dia:
            rep_date = st.date_input("Dia do Treino", datetime.today(), key="main_dp")
            d_str = rep_date.strftime("%Y-%m-%d")
            conn = get_db_connection()
            sess = pd.read_sql_query("SELECT * FROM sessions WHERE user_id=? AND start_date=?", conn, params=(user, d_str))
            gks = pd.read_sql_query("SELECT id, name FROM goalkeepers WHERE user_id=?", conn, params=(user,))
            existing = pd.read_sql_query("SELECT gk_id, rating, notes FROM training_ratings WHERE user_id=? AND date=?", conn, params=(user, d_str))
            conn.close()
            ex_map = {}
            if not existing.empty:
                for _, r in existing.iterrows(): ex_map[int(r['gk_id'])] = {'r': r['rating'], 'n': r['notes']}
            if not sess.empty:
                s_data = sess.iloc[0]
                st.info(f"**{s_data['type']}** | {s_data['title']}")
                drills = parse_drills(s_data['drills_list'])
                if drills:
                    txt_list = [f"{d['title']} ({d['sets']}x{d['reps']})" if d['sets'] else d['title'] for d in drills]
                    st.caption(f"📋 Plano: {', '.join(txt_list)}")
                with st.form("daily_rep"):
                    st.markdown("### Análise da Sessão")
                    r_txt = st.text_area("Relatório do Treinador", value=s_data['report'] if s_data['report'] else "")
                    st.markdown("### Notas Individuais")
                    r_save = {}
                    n_save = {}
                    if not gks.empty:
                        for _, gk in gks.iterrows():
                            gid = int(gk['id'])
                            d_r = ex_map[gid]['r'] if gid in ex_map else 5
                            d_n = ex_map[gid]['n'] if gid in ex_map else ""
                            with st.expander(f"{gk['name']}"):
                                c1, c2 = st.columns([1,3])
                                with c1: r_save[gid] = st.slider("Nota", 1, 10, int(d_r), key=f"sl_{gid}_{d_str}")
                                with c2: n_save[gid] = st.text_input("Obs", value=d_n, key=f"tx_{gid}_{d_str}")
                    if st.form_submit_button("Guardar Relatório e Notas"):
                        conn = get_db_connection()
                        c = conn.cursor()
                        c.execute("UPDATE sessions SET report=? WHERE id=?", (r_txt, int(s_data['id'])))
                        for gid, val in r_save.items():
                            c.execute("DELETE FROM training_ratings WHERE user_id=? AND date=? AND gk_id=?", (user, d_str, gid))
                            c.execute("INSERT INTO training_ratings (user_id, date, gk_id, rating, notes) VALUES (?,?,?,?,?)", (user, d_str, gid, val, n_save[gid]))
                        conn.commit(); conn.close(); st.success("Guardado!"); st.rerun()
            else: st.warning("Sem sessão para este dia.")
        with tab_sem:
            conn = get_db_connection()
            micros = pd.read_sql_query("SELECT * FROM microcycles WHERE user_id=? ORDER BY start_date DESC", conn, params=(user,))
            conn.close()
            if not micros.empty:
                sel_m = st.selectbox("Escolher Semana", micros['title'].unique())
                m_data = micros[micros['title'] == sel_m].iloc[0]
                st.info(f"Objetivo: {m_data['goal']}")
                with st.form("weekly_rep_form"):
                    wr = st.text_area("Relatório Semanal", value=m_data['report'] if m_data['report'] else "", height=200)
                    if st.form_submit_button("Guardar Semanal"):
                        conn = get_db_connection()
                        conn.cursor().execute("UPDATE microcycles SET report=? WHERE id=?", (wr, int(m_data['id'])))
                        conn.commit(); conn.close(); st.success("Guardado!"); st.rerun()
            else: st.warning("Cria semanas primeiro.")

    # --- 3. EVOLUÇÃO ---
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

    # --- 4. CENTRO DE JOGO ---
    elif menu == "Centro de Jogo":
        st.header("🏟️ Ficha de Jogo (Completa)")
        conn = get_db_connection()
        games = pd.read_sql_query("SELECT start_date, title FROM sessions WHERE user_id=? AND type='Jogo' ORDER BY start_date DESC", conn, params=(user,))
        gks = pd.read_sql_query("SELECT id, name FROM goalkeepers WHERE user_id=?", conn, params=(user,))
        conn.close()
        if not games.empty:
            game_opt = [f"{r['start_date']} | {r['title']}" for _, r in games.iterrows()]
            sel_game = st.selectbox("Jogo", game_opt)
            sel_date = sel_game.split(" | ")[0]
            sel_opp = sel_game.split(" | ")[1]
            st.markdown("---")
            with st.form("match_stats"):
                # Header
                c1, c2, c3, c4, c5 = st.columns(5)
                gk = c1.selectbox("GR", gks['name'].tolist() if not gks.empty else [])
                rt = c2.slider("Nota", 1, 10, 5)
                res = c3.text_input("Resultado")
                gls = c4.number_input("Golos Sofridos", 0, 20)
                svs = c5.number_input("Defesas", 0, 50)

                with st.expander("🧱 1. DEFESA DE BALIZA: BLOQUEIOS"):
                    b1, b2 = st.columns(2)
                    with b1:
                        bloq_sq_r = st.number_input("Rasteiro (SQ)", 0, 20, key="b1")
                        bloq_sq_m = st.number_input("Médio (SQ)", 0, 20, key="b2")
                        bloq_sq_a = st.number_input("Alto (SQ)", 0, 20, key="b3")
                    with b2:
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
                    du_fro = st.number_input("Frontal", 0, 20)

                with st.expander("🎯 7. DISTRIBUIÇÃO (TÁTICA)"):
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

                rep = st.text_area("Relatório Final")
                
                if st.form_submit_button("Guardar Ficha de Jogo"):
                    conn = get_db_connection()
                    gid = int(gks[gks['name']==gk].iloc[0]['id']) if not gks.empty else 0
                    c = conn.cursor()
                    c.execute("DELETE FROM matches WHERE user_id=? AND date=?", (user, sel_date))
                    
                    # CORREÇÃO CRÍTICA DO ERRO SQL (Lista explícita para evitar mismatch)
                    # Geração dinâmica dos ? para não falhar
                    vals = (
                        user, sel_date, sel_opp, gid, gls, svs, res, rep, rt, # 9
                        bloq_sq_r, bloq_sq_m, bloq_sq_a, bloq_cq_r, bloq_cq_m, bloq_cq_a, # 6
                        rec_sq_m, rec_sq_a, rec_cq_r, rec_cq_m, rec_cq_a, rec_cq_v, # 6
                        desv_sq_p, desv_sq_mf, desv_sq_ml, desv_sq_a1, desv_sq_a2, # 5
                        desv_cq_v, desv_cq_r1, desv_cq_r2, desv_cq_a1, desv_cq_a2, # 5
                        ext_rec, ext_d1, ext_d2, voo_rec, voo_d1, voo_d2, voo_dmc, # 7
                        de_cab, de_car, de_ali, de_rec, # 4
                        du_par, du_aba, du_est, du_fro, # 4
                        pa_c1, pa_c2, pa_l1, pa_l2, di_cm, di_lm, di_pm, di_vo, di_cp, di_lp, # 10
                        cr_rec, cr_s1, cr_s2, cr_int, # 4
                        eto_pb_curto, eto_pb_medio, eto_pb_longo # 3
                    )
                    
                    placeholders = ",".join(["?"] * len(vals))
                    
                    c.execute(f'''INSERT INTO matches VALUES (NULL, {placeholders})''', vals)
                    conn.commit(); conn.close(); st.success("Ficha Guardada com Sucesso!")
                    st.rerun()
            
            # --- HISTÓRICO VISÍVEL (NOVO) ---
            st.markdown("---")
            st.subheader("Histórico de Jogos Guardados")
            conn = get_db_connection()
            hist = pd.read_sql_query("SELECT date, opponent, result, rating, goals_conceded FROM matches WHERE user_id=? ORDER BY date DESC", conn, params=(user,))
            conn.close()
            if not hist.empty:
                st.dataframe(hist, use_container_width=True)
            else:
                st.info("Ainda sem jogos registados.")

        else: st.info("Marca jogos primeiro.")

    # --- 5. CALENDÁRIO ---
    elif menu == "Calendário":
        st.header("📅 Calendário")
        conn = get_db_connection()
        sess = pd.read_sql_query("SELECT type, title, start_date FROM sessions WHERE user_id=?", conn, params=(user,))
        conn.close()
        evs = []
        for _, r in sess.iterrows():
            c = "#3788d8"
            if r['type']=="Jogo": c="#d9534f"
            elif r['type']=="Descanso": c="#28a745"
            evs.append({"title": r['title'], "start": r['start_date'], "end": r['start_date'], "backgroundColor": c})
        calendar(events=evs, options={"initialView": "dayGridMonth"})

    # --- 6. ATLETAS ---
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
                conn = get_db_connection()
                conn.cursor().execute("DELETE FROM goalkeepers WHERE id=?", (e_id,))
                conn.commit(); conn.close(); st.success("Apagado"); st.rerun()
        
        elif mode!="Eliminar":
            with st.form("gk_form"):
                st.subheader("1. Perfil")
                c1,c2,c3 = st.columns(3)
                nm = c1.text_input("Nome", value=d_n)
                ag = c2.number_input("Idade", 0, 50, value=d_a)
                stt = c3.selectbox("Estado", ["Apto", "Lesionado"], index=0)
                st.subheader("2. Biometria")
                b1,b2,b3,b4,b5=st.columns(5)
                ht=b1.number_input("Altura", 0.0, 250.0, value=d_h)
                ws=b2.number_input("Envergadura", 0.0, 250.0, value=d_w)
                al=b3.number_input("Braço E", 0.0, 150.0, value=d_al)
                ar=b4.number_input("Braço D", 0.0, 150.0, value=d_ar)
                gl=b5.text_input("Luva", value=d_gl)
                st.subheader("3. Saltos")
                j1,j2,j3=st.columns(3)
                jf2=j1.number_input("Frontal 2", 0.0, value=d_jf2)
                jfl=j2.number_input("Frontal E", 0.0, value=d_jfl)
                jfr=j3.number_input("Frontal D", 0.0, value=d_jfr)
                j4,j5=st.columns(2)
                jll=j4.number_input("Lateral E", 0.0, value=d_jll)
                jlr=j5.number_input("Lateral D", 0.0, value=d_jlr)
                st.subheader("4. Testes")
                t1,t2,t3=st.columns(3)
                tr=t1.text_input("Resistência", value=d_tr)
                ta=t2.text_input("Agilidade", value=d_ta)
                tv=t3.text_input("Velocidade", value=d_tv)
                
                if st.form_submit_button("Guardar"):
                    conn = get_db_connection()
                    c = conn.cursor()
                    if mode=="Novo":
                        c.execute('''INSERT INTO goalkeepers (user_id, name, age, status, height, wingspan, arm_len_left, arm_len_right, glove_size, jump_front_2, jump_front_l, jump_front_r, jump_lat_l, jump_lat_r, test_res, test_agil, test_vel) VALUES (?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?)''', 
                                  (user, nm, ag, stt, ht, ws, al, ar, gl, jf2, jfl, jfr, jll, jlr, tr, ta, tv))
                    elif e_id:
                        c.execute('''UPDATE goalkeepers SET name=?, age=?, status=?, height=?, wingspan=?, arm_len_left=?, arm_len_right=?, glove_size=?, jump_front_2=?, jump_front_l=?, jump_front_r=?, jump_lat_l=?, jump_lat_r=?, test_res=?, test_agil=?, test_vel=? WHERE id=?''', 
                                  (nm, ag, stt, ht, ws, al, ar, gl, jf2, jfl, jfr, jll, jlr, tr, ta, tv, e_id))
                    conn.commit(); conn.close(); st.success("Guardado"); st.rerun()
        if not all_gks.empty: st.dataframe(all_gks.drop(columns=['user_id', 'notes']), use_container_width=True)

    # --- 7. EXERCÍCIOS ---
    elif menu == "Exercícios":
        st.header("⚽ Biblioteca Técnica")
        if 'edit_drill_id' not in st.session_state: st.session_state['edit_drill_id'] = None
        conn = get_db_connection()
        all_ex = pd.read_sql_query("SELECT * FROM exercises WHERE user_id=?", conn, params=(user,))
        conn.close()
        
        d_tit, d_mom, d_typ, d_desc, d_obj, d_mat, d_spa = "", "Defesa de Baliza", "Técnico", "", "", "", ""
        if st.session_state['edit_drill_id'] and not all_ex.empty:
            edit_row = all_ex[all_ex['id'] == st.session_state['edit_drill_id']].iloc[0]
            d_tit = edit_row['title']; d_mom = edit_row['moment']; d_typ = edit_row['training_type']
            d_desc = edit_row['description']; d_obj = edit_row['objective'] if edit_row['objective'] else ""
            d_mat = edit_row['materials'] if edit_row['materials'] else ""; d_spa = edit_row['space'] if edit_row['space'] else ""
            st.warning(f"✏️ A Editar: {d_tit}")
            if st.button("❌ Cancelar"): st.session_state['edit_drill_id'] = None; st.rerun()

        with st.form("drill_form"):
            st.subheader("Atualizar Exercício" if st.session_state['edit_drill_id'] else "Novo Exercício")
            title = st.text_input("Título", value=d_tit)
            moms = ["Defesa de Baliza", "Defesa do Espaço", "Cruzamento", "Duelos", "Distribuição", "Passe Atrasado"]
            typs = ["Técnico", "Tático", "Técnico-Tático", "Físico", "Psicológico"]
            c1, c2, c3 = st.columns(3)
            moment = c1.selectbox("Momento", moms, index=moms.index(d_mom) if d_mom in moms else 0)
            train_type = c2.selectbox("Tipo", typs, index=typs.index(d_typ) if d_typ in typs else 0)
            space = c3.text_input("Espaço", value=d_spa)
            c4, c5 = st.columns(2)
            objective = c4.text_input("Objetivo", value=d_obj)
            materials = c5.text_area("Material", value=d_mat, height=100)
            desc = st.text_area("Descrição", value=d_desc, height=150)
            img = st.file_uploader("Imagem", type=['png','jpg'])
            
            if st.form_submit_button("Guardar"):
                b_img = img.read() if img else None
                conn = get_db_connection()
                c = conn.cursor()
                if not st.session_state['edit_drill_id']:
                    c.execute('''INSERT INTO exercises (user_id, title, moment, training_type, description, objective, materials, space, image) 
                                 VALUES (?,?,?,?,?,?,?,?,?)''', (user, title, moment, train_type, desc, objective, materials, space, b_img))
                    st.success("Criado!")
                else:
                    eid = st.session_state['edit_drill_id']
                    if b_img: c.execute('''UPDATE exercises SET title=?, moment=?, training_type=?, description=?, objective=?, materials=?, space=?, image=? WHERE id=?''', (title, moment, train_type, desc, objective, materials, space, b_img, eid))
                    else: c.execute('''UPDATE exercises SET title=?, moment=?, training_type=?, description=?, objective=?, materials=?, space=? WHERE id=?''', (title, moment, train_type, desc, objective, materials, space, eid))
                    st.success("Atualizado!")
                    st.session_state['edit_drill_id'] = None
                conn.commit(); conn.close(); st.rerun()

        st.markdown("---")
        st.subheader("Catálogo")
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
                                        conn = get_db_connection()
                                        conn.cursor().execute("DELETE FROM exercises WHERE id=?", (r['id'],))
                                        conn.commit(); conn.close(); st.rerun()
                                with c_txt:
                                    st.write(f"**Obj:** {r['objective']}"); st.write(f"**Mat:** {r['materials']}")
                                    st.caption(r['description'])
                                with c_img:
                                    if r['image']: st.image(r['image'])
                    else: st.info("Vazio.")
        else:
            for t in tabs: t.info("Vazio.")

if st.session_state['logged_in']:
    main_app()
else:
    login_page()