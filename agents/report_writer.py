from litellm import completion

from agents.state import UnderwritingState


def write_report(state: UnderwritingState) -> UnderwritingState:
    prompt = f"""You are a credit risk analyst assistant working for a financial institution.                                                                                 
    Your task is to write a concise loan decision report for a human credit analyst based on the automated underwriting system output.                           
                                                                                                                                                                
    The report should:                                                                                                                                           
    - State the final decision clearly at the top                                                                                                                
    - Explain the key risk factors using the SHAP summary                                                                                                        
    - Mention any flags that were raised and what they mean                                                                                                      
    - Be professional, factual, and easy to read                                                                                                                 
    - Be no longer than 200 words                                                                                                                                
                                                                                                                                                                
    Here is the underwriting output:                                                                                                                             
                                                    
    Decision: {state.decision}                                                                                                                                   
    Probability of Default: {state.probability_of_default:.2%}
    Income Flag (debt-to-income ratio exceeded threshold): {state.income_flag}
    Consistency Flag (inconsistencies detected between declared data and credit bureau): {state.consistency_flag}                                                
                                                                                                                                                                
    SHAP Analysis:                                                                                                                                               
    {state.shap_summary}"""

    response = completion(
        model="ollama/gemma4:latest",
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.choices[0].message.content

    return state.model_copy(update={"report": text})
