from src.controllers.controller import PipelineController

def main():
    """Execução principal do Pipeline."""
    controller = PipelineController()

    controller.check_database_status()

    controller.run_full_pipeline()

    controller.check_database_status()

if __name__ == "__main__":
    main()