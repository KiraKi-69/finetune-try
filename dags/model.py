import json
import triton_python_backend_utils as pb_utils
import numpy as np
from transformers import pipeline
import logging

class TritonPythonModel:
    def initialize(self, args):
        self.generator = pipeline("text-generation", model="sberbank-ai/rugpt3small_based_on_gpt2")

    def execute(self, requests):
        responses = []
        print("This is requests")
        logging.info(requests)
        print("-"*10)
        for request in requests:
            print("This is reques")
            logging.info(request)
            print("-"*10)
            input = pb_utils.get_input_tensor_by_name(request, "text")
            input_string = input.as_numpy()[0].decode()
            pipeline_output = self.generator(input_string, do_sample=True, max_length=200)
            generated_txt = pipeline_output[0]["generated_text"]
            output = generated_txt
        
            inference_response = pb_utils.InferenceResponse(
                output_tensors=[
                    pb_utils.Tensor(
                        "answer",
                        np.array([output.encode()]),
                    )
                ]
            )
            responses.append(inference_response)
            
        return responses

    def finalize(self, args):
         self.generator = None