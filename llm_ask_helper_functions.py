import json
import requests

def get_acces_token_ai_core(client_id, client_secret, auth_url):
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(auth_url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def ask_llm(question, token, genai_url):
    headers = {
        "Authorization": f"Bearer {token}",
        "DataServiceVersion": "2.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "AI-Resource-Group": "Datasphere-copilot"
    }
    payload = {
        "config": {
            "modules":{
                "prompt_templating":{
                    "prompt": {
                        "template": [
                                    {
                                        "role": "user",
                                        "content": [
                                            {
                                                "type": "text",
                                                "text": "You are an SAP Datasphere documentation expert.\nContext: {{?inputContext}}\nBelow are documentation fragments (PDF):\n{{?grounding_output_variable}}\nUser question: {{?grounding_input_variable_1}}\nProvide a complete and accurate answer if the information is present. If the answer is not present, say so."
                                            }
                                        ]
                                }
                    ],
                    "defaults": {
                    "grounding_input_variable_1": question
                    }
                    },
                    "model": {
                        "name": "gemini-2.5-pro",
                        "params": {
                            "temperature": 0.25
                        },
                        "version": "001"
                    }

                },
                "grounding": {
                    "type": "document_grounding_service",
                    "config": {
                        "filters": [
                            {
                                "id": "filter1",
                                "data_repositories": [
                                    "97943fff-ccec-4fbc-984c-354389899072" 
                                ],
                                "search_config": {
                                    "max_chunk_count": 250
                                },
                                "data_repository_type": "vector"
                            }
                        ],
                        "placeholders":{
                            "input": [
                                "grounding_input_variable_1"
                            ],
                            "output": "grounding_output_variable"
                        }
                        
                    }
                }
                
            }
        }
        ,
        "placeholder_values": 
        {
            "grounding_input_variable_1": question,
            "inputContext": "Datasphere Consultant"
        }
    }

    resp = requests.post(genai_url, headers=headers, json=payload)
    print("Status Code:", resp.status_code)        # e.g. 200, 400
    print("Response Headers:", resp.headers)      
    print("Raw Text:", resp.text)                 

    if resp.status_code != 200:
        return f"LLM call failed: {resp.status_code} {resp.text}"
    data = resp.json()
    return data["final_result"]["choices"][0]["message"]["content"]