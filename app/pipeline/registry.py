# app/pipeline/registry.py
from app.pipeline.nhanes_pipeline import NHANESPipeline
from app.pipeline.forian_claims_pipeline import FORIANCLAIMSPipeline

PIPELINE_REGISTRY = {
    "nhanes": NHANESPipeline(),
    "forian_claims": FORIANCLAIMSPipeline(),
}
