#App de Geolocalização Reversa
#Desenvolvido por Thiago Barros
#Versão 1.0



import os
import threading
import time
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
import requests
import configparser
import sqlite3
import unicodedata


URL_IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
URL_NOMINATIM = "https://nominatim.openstreetmap.org/reverse"

DEFAULT_CONFIGS = {
    "pasta_temp": "malhas_ibge",
    "input": "",
    "output": "coordenadas_com_ibge",
    "batch_size": "10",
    "delay_segundos": "1.5",
    "user_agent": "MeuAppGeocodificacao/1.0 (contato@exemplo.com)",
    "formato_saida": "xlsx",
}

CONFIG_FILE = ".conf"

def load_configs():
    config = configparser.ConfigParser()
    if os.path.exists(CONFIG_FILE):
        config.read(CONFIG_FILE)
        if 'CONFIGS' in config:
            cfgs = dict(config['CONFIGS'])
            if 'formato_saida' not in cfgs:
                cfgs['formato_saida'] = DEFAULT_CONFIGS['formato_saida']
            return cfgs
    return DEFAULT_CONFIGS.copy()

def save_configs(configs):
    config = configparser.ConfigParser()
    config['CONFIGS'] = configs
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        config.write(f)


def normalizar(texto):
    """
    Remove acentos e converte o texto para minúsculas para facilitar comparações.
    """
    if texto is None:
        return ""
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8').lower()

import requests

def geocodificar_reversa(latitude, longitude, user_agent, url_nominatim=URL_NOMINATIM):
    """
    Realiza a geocodificação reversa usando o Nominatim e retorna um dicionário com os componentes do endereço.
    """
    try:
        headers = {"User-Agent": user_agent}

        params = {
            "format": "json",
            "lat": latitude,
            "lon": longitude,
            "addressdetails": 1,
            "zoom": 18,
            "accept-language": "pt-BR"
        }

        response = requests.get(url_nominatim, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        endereco = data.get("address", {})
        return endereco

    except Exception as e:
        print(f"[ERRO] Geocodificação reversa falhou para ({latitude}, {longitude}): {e}")
        return None

def buscar_dados_ibge(cidade, uf_sigla, url_ibge=URL_IBGE):
    """
    Busca os dados do município no IBGE, retornando nome, código IBGE, mesorregião e UF.
    Faz comparação com nomes normalizados (sem acentos e em minúsculas).
    """
    try:
        response = requests.get(url_ibge, timeout=25)
        response.raise_for_status()
        municipios = response.json()

        cidade_normalizada = normalizar(cidade)
        uf_sigla_normalizada = normalizar(uf_sigla)

        for municipio in municipios:
            nome_mun = normalizar(municipio["nome"])
            uf = municipio["microrregiao"]["mesorregiao"]["UF"]
            sigla_uf_api = normalizar(uf["sigla"])

            if nome_mun == cidade_normalizada and sigla_uf_api == uf_sigla_normalizada:
                municipio_ibge = municipio["nome"]
                codigo_ibge = municipio["id"]
                mesorregiao = municipio["microrregiao"]["mesorregiao"]["nome"]
                codigo_mesorregiao = municipio["microrregiao"]["mesorregiao"]["id"]
                uf_nome = uf["nome"]
                uf_codigo = uf["id"]
                uf_sigla = uf["sigla"]

                return (municipio_ibge, codigo_ibge, mesorregiao, codigo_mesorregiao,
                        uf_nome, uf_codigo, uf_sigla)

        return ("", "", "", "", "", "", "")

    except Exception as e:
        print(f"[ERRO] Buscando dados IBGE: {e}")
        return ("", "", "", "", "", "", "")

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Geocodificação IBGE")
        self.geometry("750x600")  # aumentei a altura para o radio buttons
        self.resizable(False, False)

        self.configs = load_configs()
        self.config_entries = {}
        self.df = None
        self.lock = threading.Lock()
        self.processed_lines = 0
        self.total_lines = 0
        self.pause_flag = threading.Event()
        self.pause_flag.set()  # rodando
        self.cancel_flag = threading.Event()
        self.cancel_flag.clear()

        self.tempo_inicio = None  # para calcular tempo estimado

        self.create_widgets()
        try:
            icone = tk.PhotoImage(file="logo.png")
            self.iconphoto(False, icone)
            self._icone = icone
        except Exception as e:
            print(f"Falha ao carregar ícone: {e}")

        self.update_bolinha("red")

    def create_widgets(self):
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        frame_controle = ttk.Frame(notebook)
        notebook.add(frame_controle, text="Controle / Log")

        frame_botoes = ttk.Frame(frame_controle)
        frame_botoes.pack(fill=tk.X, pady=5)

        self.canvas_bolinha = tk.Canvas(frame_botoes, width=20, height=20, highlightthickness=0)
        self.bolinha_id = self.canvas_bolinha.create_oval(2, 2, 18, 18, fill="red")
        self.canvas_bolinha.pack(side=tk.LEFT, padx=(0, 5))

        self.btn_processar = ttk.Button(frame_botoes, text="Processar", command=self.iniciar_processamento, state="disabled")
        self.btn_processar.pack(side=tk.LEFT, padx=5)

        self.btn_pause = ttk.Button(frame_botoes, text="Pausar", command=self.toggle_pause, state="disabled")
        self.btn_pause.pack(side=tk.LEFT, padx=5)

        self.btn_cancelar = ttk.Button(frame_botoes, text="Cancelar", command=self.cancelar_processamento, state="disabled")
        self.btn_cancelar.pack(side=tk.LEFT, padx=5)

        self.btn_load = ttk.Button(frame_botoes, text="Selecionar arquivo CSV/XLSX", command=self.selecionar_arquivo)
        self.btn_load.pack(side=tk.LEFT, padx=20)

        self.label_status = ttk.Label(frame_controle, text="Status: Aguardando arquivo...")
        self.label_status.pack(fill=tk.X, padx=10, pady=5)

        self.progress_var = tk.DoubleVar()
        self.progressbar = ttk.Progressbar(frame_controle, variable=self.progress_var, maximum=100)
        self.progressbar.pack(fill=tk.X, padx=10, pady=5)

        self.label_tempo_estimado = ttk.Label(frame_controle, text="Tempo estimado restante: N/A")
        self.label_tempo_estimado.pack(fill=tk.X, padx=10, pady=2)

        self.text_log = tk.Text(frame_controle, height=15, state="normal")
        self.text_log.pack(fill=tk.BOTH, padx=10, pady=5, expand=True)
        
        footer_text = "Desenvolvido por Thiago Barros | Versão 1.0 | © 2025"
        footer_label = ttk.Label(self, text=footer_text, anchor="center", foreground="gray")
        footer_label.pack(side=tk.BOTTOM, fill=tk.X, pady=(2, 5))

        frame_formato = ttk.LabelFrame(frame_controle, text="Formato de saída")
        frame_formato.pack(fill=tk.X, padx=10, pady=5)

        self.formato_saida_var = tk.StringVar(value=self.configs.get("formato_saida", "xlsx"))

        formatos = [("Excel (.xlsx)", "xlsx"),
                    ("CSV (.csv)", "csv"),
                    ("JSON (.json)", "json"),
                    ("SQLite (.sql)", "sql")]

        for text, val in formatos:
            rb = ttk.Radiobutton(frame_formato, text=text, variable=self.formato_saida_var, value=val)
            rb.pack(side=tk.LEFT, padx=10, pady=5)
        # ----------------------------------

        frame_config = ttk.Frame(notebook)
        notebook.add(frame_config, text="Configurações")

        linhas = [
            ("Pasta Temp", "pasta_temp"),
            ("Arquivo Entrada (CSV/XLSX)", "input_csv"),
            ("Arquivo Saída", "output"),
            ("Batch Size (linhas por lote)", "batch_size"),
            ("Delay entre lotes (segundos)", "delay_segundos"),
            ("User-Agent (Nominatim)", "user_agent"),
            ("URL da API do IBGE", "api_ibge_url"),
            ("URL da API do Nominatim", "api_nominatim_url"),
        ]

        for i, (label_text, key) in enumerate(linhas):
            label = ttk.Label(frame_config, text=label_text)
            label.grid(row=i, column=0, sticky=tk.W, padx=5, pady=4)
            entry = ttk.Entry(frame_config, width=50)
            entry.grid(row=i, column=1, sticky=tk.EW, padx=5, pady=4)
            entry.insert(0, self.configs.get(key, ""))
            self.config_entries[key] = entry

        frame_config.columnconfigure(1, weight=1)

        legenda_texto = (
            "Legenda:\n"
            "- Pasta Temp: pasta para arquivos temporários (não obrigatória).\n"
            "- Arquivo Entrada: arquivo CSV ou XLSX contendo colunas 'latitude' e 'longitude'.\n"
            "- Arquivo Saída: nome do arquivo de saída.\n"
            "- Batch Size: número de linhas processadas por vez.\n"
            "- Delay entre lotes: segundos de espera entre cada lote para respeitar limites da API.\n"
            "- User-Agent: identificador para requisições Nominatim.\n"
            "- URL da API do IBGE: endpoint para consulta dos municípios.\n"
            "- URL da API do Nominatim: endpoint para geocodificação reversa."
        )
        label_legenda = ttk.Label(frame_config, text=legenda_texto, justify=tk.LEFT, foreground="gray")
        label_legenda.grid(row=len(linhas), column=0, columnspan=2, sticky=tk.W, padx=5, pady=10)

    def update_bolinha(self, cor):
        self.canvas_bolinha.itemconfig(self.bolinha_id, fill=cor)

    def log(self, msg):
        texto = f"[{time.strftime('%H:%M:%S')}] {msg}"
        print(texto)
        self.text_log.config(state="normal")
        self.text_log.insert(tk.END, texto + "\n")
        self.text_log.see(tk.END)
        self.text_log.config(state="disabled")

    def selecionar_arquivo(self):
        caminho = filedialog.askopenfilename(
            title="Selecione arquivo CSV ou XLSX",
            filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx")]
        )
        if caminho:
            self.config_entries["input_csv"].delete(0, tk.END)
            self.config_entries["input_csv"].insert(0, caminho)
            self.log(f"Arquivo selecionado: {caminho}")
            self.label_status.config(text=f"Arquivo: {os.path.basename(caminho)}")
            self.btn_processar.config(state="normal")
            self.update_bolinha("blue")

    def atualizar_progresso(self):
        if self.total_lines > 0:
            perc = (self.processed_lines / self.total_lines) * 100
            self.progress_var.set(perc)
        else:
            self.progress_var.set(0)
        self.update()

    def toggle_pause(self):
        if self.pause_flag.is_set():
            self.pause_flag.clear()
            self.log("Processamento pausado.")
            self.btn_pause.config(text="Retomar")
            self.update_bolinha("yellow")
        else:
            self.pause_flag.set()
            self.log("Processamento retomado.")
            self.btn_pause.config(text="Pausar")
            self.update_bolinha("green")

    def cancelar_processamento(self):
        if messagebox.askyesno("Confirmar Cancelamento", "Deseja realmente cancelar o processamento?"):
            self.cancel_flag.set()
            self.log("Processamento cancelado pelo usuário.")
            self.btn_pause.config(state="disabled")
            self.btn_cancelar.config(state="disabled")
            self.btn_processar.config(state="normal")
            self.update_bolinha("red")
            self.label_status.config(text="Status: Cancelado.")
            self.label_tempo_estimado.config(text="Tempo estimado restante: N/A")

    def iniciar_processamento(self):
        if not self.config_entries["input_csv"].get():
            messagebox.showerror("Erro", "Selecione um arquivo de entrada.")
            return

        self.btn_processar.config(state="disabled")
        self.btn_pause.config(state="normal")
        self.btn_cancelar.config(state="normal")  # Habilita o cancelar
        self.pause_flag.set()
        self.cancel_flag.clear()
        self.processed_lines = 0
        self.progress_var.set(0)
        self.label_tempo_estimado.config(text="Tempo estimado restante: Calculando...")
        self.text_log.config(state="normal")
        self.text_log.delete("1.0", tk.END)
        self.text_log.config(state="disabled")

        # Atualiza configs
        for chave, entry in self.config_entries.items():
            self.configs[chave] = entry.get()
        # Salva também o formato de saída selecionado
        self.configs["formato_saida"] = self.formato_saida_var.get()
        save_configs(self.configs)

        self.tempo_inicio = time.time()  # marca início do processamento

        thread = threading.Thread(target=self.processar_arquivo_entrada, daemon=True)
        thread.start()

    def get_batch_size_delay(self):
        try:
            batch_size = int(self.configs.get("batch_size", DEFAULT_CONFIGS["batch_size"]))
        except:
            batch_size = int(DEFAULT_CONFIGS["batch_size"])

        try:
            delay = float(self.configs.get("delay_segundos", DEFAULT_CONFIGS["delay_segundos"]))
        except:
            delay = float(DEFAULT_CONFIGS["delay_segundos"])

        return batch_size, delay

    def processar_arquivo_entrada(self):
        caminho = self.configs.get("input_csv", "")
        extensao = os.path.splitext(caminho)[1].lower()

        try:
            if extensao == ".csv":
                self.df = pd.read_csv(caminho)
            elif extensao == ".xlsx":
                self.df = pd.read_excel(caminho)
            else:
                raise ValueError("Formato de arquivo não suportado.")
        except Exception as e:
            self.log(f"Erro ao carregar arquivo: {e}")
            messagebox.showerror("Erro", f"Erro ao carregar arquivo:\n{e}")
            self.btn_processar.config(state="normal")
            self.btn_pause.config(state="disabled")
            self.btn_cancelar.config(state="disabled")
            return

        if "latitude" not in self.df.columns or "longitude" not in self.df.columns:
            messagebox.showerror("Erro", "O arquivo deve conter colunas chamadas 'latitude' e 'longitude'.")
            self.btn_processar.config(state="normal")
            self.btn_pause.config(state="disabled")
            self.btn_cancelar.config(state="disabled")
            return

        # Criar colunas para dados IBGE
        for col in ["cidade", "estado", "municipio_ibge", "codigo_ibge", "mesorregiao",
                    "codigo_mesorregiao", "uf_nome", "uf_codigo", "uf_sigla"]:
            self.df[col] = ""

        self.total_lines = len(self.df)
        self.processed_lines = 0
        batch_size, delay = self.get_batch_size_delay()
        user_agent = self.configs.get("user_agent", DEFAULT_CONFIGS["user_agent"])
        url_ibge = self.configs.get("api_ibge_url", URL_IBGE)
        url_nominatim = self.configs.get("api_nominatim_url", URL_NOMINATIM)

        self.log(f"Iniciando processamento de {self.total_lines} linhas, batch {batch_size}, delay {delay}s.")
        self.label_status.config(text="Status: Processando...")

        try:
            for start_idx in range(0, self.total_lines, batch_size):
                if self.cancel_flag.is_set():
                    self.log("Processamento cancelado pelo usuário.")
                    break

                self.pause_flag.wait()  # aguarda retomar se pausado

                end_idx = min(start_idx + batch_size, self.total_lines)
                batch = self.df.iloc[start_idx:end_idx]

                for idx, row in batch.iterrows():
                    self.pause_flag.wait()

                    if self.cancel_flag.is_set():
                        break

                    lat = row.get("latitude")
                    lon = row.get("longitude")

                    if pd.isna(lat) or pd.isna(lon):
                        self.log(f"[{idx}] Coordenadas ausentes. Pulando.")
                        print(f"Linha {idx} - sem localização")  # mensagem no terminal
                        with self.lock:
                            self.processed_lines += 1
                        self.atualizar_progresso()
                        self.atualizar_tempo_estimado()
                        continue

                    endereco = geocodificar_reversa(lat, lon, user_agent, url_nominatim)
                    if not endereco:
                        self.log(f"[{idx}] Falha na geocodificação reversa ({lat}, {lon}).")
                        print(f"Linha {idx} - sem localização")  # mensagem no terminal
                        with self.lock:
                            self.processed_lines += 1
                        self.atualizar_progresso()
                        self.atualizar_tempo_estimado()
                        continue

                    cidade = (endereco.get("city") or endereco.get("town") or endereco.get("village") or
                              endereco.get("cidade") or "")
                    estado = (endereco.get("state_code") or endereco.get("state") or endereco.get("estado") or "")

                    self.df.at[idx, "cidade"] = cidade
                    self.df.at[idx, "estado"] = estado

                    if cidade and estado:
                        dados_ibge = buscar_dados_ibge(cidade, estado[:2], url_ibge)
                        (
                            municipio_ibge,
                            codigo_ibge,
                            mesorregiao,
                            codigo_mesorregiao,
                            uf_nome,
                            uf_codigo,
                            uf_sigla,
                        ) = dados_ibge

                        self.df.at[idx, "municipio_ibge"] = municipio_ibge or ""
                        self.df.at[idx, "codigo_ibge"] = codigo_ibge or ""
                        self.df.at[idx, "mesorregiao"] = mesorregiao or ""
                        self.df.at[idx, "codigo_mesorregiao"] = codigo_mesorregiao or ""
                        self.df.at[idx, "uf_nome"] = uf_nome or ""
                        self.df.at[idx, "uf_codigo"] = uf_codigo or ""
                        self.df.at[idx, "uf_sigla"] = uf_sigla or ""

                        self.log(f"[{idx}] {cidade}, {estado} -> {municipio_ibge} ({codigo_ibge})")
                    else:
                        self.log(f"[{idx}] Cidade/Estado não encontrados para ({lat}, {lon})")

                    print(f"Linha {idx} - Cidade: {cidade} - Estado: {estado}")

                    with self.lock:
                        self.processed_lines += 1
                    self.atualizar_progresso()
                    self.atualizar_tempo_estimado()

                if self.cancel_flag.is_set():
                    break

                # Delay com pausa respeitada
                for _ in range(int(delay * 10)):
                    if self.cancel_flag.is_set():
                        break
                    self.pause_flag.wait()
                    time.sleep(0.1)

            self.label_status.config(text="Status: Finalizado")

        except Exception as e:
            self.log(f"Erro durante processamento: {e}")
            messagebox.showerror("Erro", f"Erro durante o processamento:\n{e}")

        finally:
            self.salvar_resultado()
            self.btn_pause.config(state="disabled")
            self.btn_cancelar.config(state="disabled")
            self.btn_processar.config(state="normal")
            self.update_bolinha("red")
            self.log("Processamento finalizado.")
            self.label_tempo_estimado.config(text="Tempo estimado restante: N/A")

    def atualizar_tempo_estimado(self):
        if self.processed_lines == 0:
            return
        elapsed = time.time() - self.tempo_inicio
        linhas_restantes = self.total_lines - self.processed_lines
        tempo_medio = elapsed / self.processed_lines
        tempo_restante = linhas_restantes * tempo_medio

        # Formatar tempo em H:M:S
        h = int(tempo_restante // 3600)
        m = int((tempo_restante % 3600) // 60)
        s = int(tempo_restante % 60)

        self.label_tempo_estimado.config(text=f"Tempo estimado restante: {h:02d}:{m:02d}:{s:02d}")
        self.update()

    def salvar_resultado(self):
        formato = self.configs.get("formato_saida", "xlsx").lower()
        output_path = self.configs.get("output", DEFAULT_CONFIGS["output"])
        if not output_path:
            output_path = DEFAULT_CONFIGS["output"]

        # Ajusta extensão de acordo com formato escolhido
        if formato == "xlsx" and not output_path.lower().endswith(".xlsx"):
            output_path += ".xlsx"
        elif formato == "csv" and not output_path.lower().endswith(".csv"):
            output_path += ".csv"
        elif formato == "json" and not output_path.lower().endswith(".json"):
            output_path += ".json"
        elif formato == "sql" and not (output_path.lower().endswith(".db") or output_path.lower().endswith(".sqlite") or output_path.lower().endswith(".sql")):
            output_path += ".db"  # extensão padrão para sqlite

        try:
            if formato == "xlsx":
                self.df.to_excel(output_path, index=False)
            elif formato == "csv":
                self.df.to_csv(output_path, index=False)
            elif formato == "json":
                self.df.to_json(output_path, orient="records", force_ascii=False, indent=2)
            elif formato == "sql":
                # Salvar em banco SQLite simples
                conn = sqlite3.connect(output_path)
                self.df.to_sql("dados_geocodificacao", conn, if_exists="replace", index=False)
                conn.close()
            else:
                # padrão xlsx
                self.df.to_excel(output_path, index=False)

            self.log(f"Arquivo salvo em: {output_path}")
        except Exception as e:
            self.log(f"Erro ao salvar arquivo: {e}")
            messagebox.showerror("Erro", f"Erro ao salvar arquivo:\n{e}")

if __name__ == "__main__":
    app = App()
    app.mainloop()





