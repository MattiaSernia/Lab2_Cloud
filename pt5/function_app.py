import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
import os
import re

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# 1. ACTIVITY: GetInputDataFn
# Legge i file dal Blob Storage e crea una lista di input [cite: 103, 104]
@app.activity_trigger(input_name="input")
def get_input_data_fn(input: str) -> list:
    # Usa la connection string da local.settings.json
    connect_str = os.getenv('MY_BLOB_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client("mrinput")
    
    all_lines = []
    
    # Itera su tutti i blob nel container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob)
        download_stream = blob_client.download_blob()
        content = download_stream.readall().decode('utf-8')
        
        # Divide in linee e crea coppie (offset/line_number, line_string)
        lines = content.splitlines()
        for i, line in enumerate(lines):
            # Aggiungiamo il nome file per rendere l'offset unico, opzionale ma utile
            key = f"{blob.name}-{i}" 
            all_lines.append((key, line))
            
    return all_lines

# 2. ACTIVITY: Mapper
# Prende (key, line), tokenizza e ridà lista di (word, 1) [cite: 87]
@app.activity_trigger(input_name="pair")
def mapper_activity(pair: tuple) -> list:
    _, line = pair # Ignoriamo la chiave (numero riga)
    words = re.findall(r'\w+', line.lower()) # Tokenize semplice
    mapped = []
    for w in words:
        mapped.append((w, 1))
    return mapped

# 3. ACTIVITY: Shuffler
# Prende le liste dai mapper e raggruppa per parola: {word: [1, 1, 1...]} [cite: 91]
@app.activity_trigger(input_name="mappedData")
def shuffler_activity(mappedData: list) -> dict:
    shuffled = {}
    # mappedData è una lista di liste (output di vari mapper)
    for sublist in mappedData:
        for key, value in sublist:
            if key not in shuffled:
                shuffled[key] = []
            shuffled[key].append(value)
    
    # Prepara l'output per il reducer: lista di tuple (word, [1,1,1])
    # Durable functions gestiscono meglio liste/dizionari JSON serializzabili
    return shuffled

# 4. ACTIVITY: Reducer
# Prende (word, [1, 1...]) e somma: (word, total) [cite: 88, 89]
@app.activity_trigger(input_name="shuffledPair")
def reducer_activity(shuffledPair: tuple) -> tuple:
    word, counts = shuffledPair
    total = sum(counts)
    return (word, total)

# 5. ORCHESTRATOR: MasterOrchestrator
# Coordina il tutto [cite: 86, 93, 94]
@app.orchestration_trigger(context_name="context")
def master_orchestrator(context: df.DurableOrchestrationContext):
    # Paso 1: Ottenere i dati (Input)
    input_data = yield context.call_activity("get_input_data_fn", "")
    
    # Passo 2: Map (Fan-out) - Eseguiamo i mapper in parallelo per ogni riga
    tasks = []
    for line_pair in input_data:
        tasks.append(context.call_activity("mapper_activity", line_pair))
    
    # Aspettiamo che tutti i mapper finiscano (Fan-in)
    mapped_results = yield context.task_all(tasks)
    
    # Passo 3: Shuffle
    # Passiamo tutti i risultati dei mapper allo shuffler
    shuffled_data_dict = yield context.call_activity("shuffler_activity", mapped_results)
    
    # Passo 4: Reduce (Fan-out) - Eseguiamo reducer per ogni parola
    reduce_tasks = []
    for word, counts in shuffled_data_dict.items():
        reduce_tasks.append(context.call_activity("reducer_activity", (word, counts)))
        
    final_results = yield context.task_all(reduce_tasks)
    
    # Convertiamo i risultati in un dizionario per una lettura più facile o li ordiniamo
    final_dict = {k: v for k, v in final_results}
    
    return final_dict

# 6. HTTP STARTER
# Trigger standard per avviare l'orchestrazione
@app.route(route="orchestrators/{functionName}") 
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client: df.DurableOrchestrationClient):
    function_name = req.route_params.get('functionName')
    instance_id = await client.start_new(function_name, None)
    response = client.create_check_status_response(req, instance_id)
    return response