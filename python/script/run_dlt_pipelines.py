def main() -> None:
    # Importar bibliotecas para paralelização
    import concurrent.futures
    import subprocess

    # Definir função que rode arquivo python
    def run_python_script(script_path: str) -> None:
        # Rodar como subprocesso python
        subprocess.run(["python", script_path])

        # Sem retorno
        return None

    # Definir a lista de caminhos dos arquivos
    arquivos = [
        "../pipelines/pipeline_chaves_na_mao.py", 
        "../pipelines/pipeline_olx.py",
        "../pipelines/pipeline_zap_imoveis.py"
        ]

    # Usando o executor multitread
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Rode os arquivos em paralelo
        executor.map(run_python_script, arquivos)

# Rodar como script
if __name__ == '__main__':
    main()