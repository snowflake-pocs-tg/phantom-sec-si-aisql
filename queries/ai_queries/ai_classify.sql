
/*
===============================================================================
AI_CLASSIFY FUNCTION - Snowflake Cortex
===============================================================================
Documentation: https://docs.snowflake.com/en/sql-reference/functions/ai_classify

The AI_CLASSIFY function uses machine learning to classify text into one of
the provided categories. It returns a JSON object with the best matching
category and a confidence score.

===============================================================================
COMPETITOR CLASSIFICATION QUERY
===============================================================================
Purpose: Identify which competitors are mentioned in sales calls
Use Case: Competitive intelligence, win/loss analysis, battlecard development

This query helps sales teams understand:
- Which competitors come up most frequently in deals
- Common competitive objections and concerns
- Opportunities to differentiate against specific competitors
- Deals at risk due to competitive evaluation

Categories:
- Secureframe: Direct competitor in compliance automation
- Drata: Direct competitor with similar features
- Sprinto: Competitor claiming 99% automation
- TrustCloud: Emerging competitor in the space
- No_Competitor_Mentioned: Calls without competitive discussion
===============================================================================
*/
SELECT
      CALL_ID as record_id,
      CHRONOLOGICAL_TRANSCRIPT as original_text,
      AI_CLASSIFY(CHRONOLOGICAL_TRANSCRIPT, 
        [   'Secureframe', 
            'Drata',
            'Sprinto', 
            'TrustCloud', 
            'No_Competitor_Mentioned'
        ]) as classification,
       CURRENT_TIMESTAMP() as classified_at
  FROM PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS
  LIMIT 10;


/*
===============================================================================
PRODUCT DIFFERENTIATOR CLASSIFICATION QUERY
===============================================================================
Purpose: Identify high-level product differentiators discussed in sales calls
Use Case: Product positioning, value prop refinement, sales enablement

This query helps teams understand:
- Which product differentiators resonate most with prospects
- Key value propositions that drive deals forward
- Areas where the product stands out from competition
- Opportunities to enhance product messaging

Categories:
- AI_Powered_Automation: AI-driven compliance automation capabilities
- Comprehensive_Integrations: 380+ integrations, broadest coverage
- Continuous_Monitoring: Real-time compliance monitoring vs manual checks
- Fast_Implementation: 4-8 week implementation vs 8-10 weeks for competitors
- Trust_Center: Customer-facing compliance portal
- Multi_Framework_Support: SOC 2, ISO 27001, etc. in one platform
- Industry_Solutions: Vertical-specific compliance solutions
- Superior_Support: 24/7 support with <2 hour response times
- No_Differentiator_Mentioned: Calls without specific differentiators
===============================================================================
*/
SELECT
      CALL_ID as record_id,
      CHRONOLOGICAL_TRANSCRIPT as original_text,
      AI_CLASSIFY(CHRONOLOGICAL_TRANSCRIPT, 
        [   'AI_Powered_Automation', 
            'Comprehensive_Integrations', 
            'Continuous_Monitoring', 
            'Fast_Implementation', 
            'Trust_Center',
            'Multi_Framework_Support', 
            'Industry_Solutions', 
            'Superior_Support',
            'No_Differentiator_Mentioned'
        ]) as classification,
       CURRENT_TIMESTAMP() as classified_at
  FROM PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS
  LIMIT 10;


/*
===============================================================================
FEATURE DIFFERENTIATOR CLASSIFICATION QUERY
===============================================================================
Purpose: Identify specific product features discussed in sales calls
Use Case: Feature adoption analysis, product roadmap prioritization, demo optimization

This query helps teams understand:
- Which specific features drive customer interest
- Technical capabilities that close deals
- Feature combinations that work well together
- Areas for product enhancement based on customer needs

Categories:
- AI_Agent: Unique AI assistant for compliance workflows
- AI_Questionnaire_Automation: 80% auto-approval rate for security questionnaires
- 90_Percent_Automation: Realistic automation of evidence collection
- 380_Plus_Integrations: Extensive integration library
- Customer_Trust_Center: Self-service compliance portal
- Multi_Framework_Compliance: Support for multiple compliance frameworks
- Migration_Services: Dedicated migration support from competitors
- 24_7_Expert_Support: Round-the-clock expert assistance
- Custom_Policy_Editor: Flexible policy creation and management
- Real_Time_Monitoring: Continuous compliance monitoring
- No_Feature_Mentioned: Calls without specific feature discussion
===============================================================================
*/
SELECT
      CALL_ID as record_id,
      CHRONOLOGICAL_TRANSCRIPT as original_text,
      AI_CLASSIFY(CHRONOLOGICAL_TRANSCRIPT, 
        [   'AI_Agent',
            'AI_Questionnaire_Automation',
            '90_Percent_Automation',
            '380_Plus_Integrations',
            'Customer_Trust_Center',
            'Multi_Framework_Compliance',
            'Migration_Services',
            '24_7_Expert_Support',
            'Custom_Policy_Editor',
            'Real_Time_Monitoring',
            'No_Feature_Mentioned'
        ]) as classification,
       CURRENT_TIMESTAMP() as classified_at
  FROM PHANTOM_SEC_POC.SI.GONG_CLEAN_TRANSCRIPTS
  LIMIT 10;