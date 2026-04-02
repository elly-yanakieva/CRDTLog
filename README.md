  # CRDTLog: A Datalog Framework for Conflict-Free Replicated Data Types                            
                                                                                                  
  This repository contains the artifact for the paper:                                            
                                                                                                  
   A Datalog Framework for Conflict-Free Replicated Data Types - 
   Elena Yanakieva, Annette Bieniusa, Stefania Dumbrava.                                         

  CRDTLog is a declarative framework for specifying and reasoning about the semantics of Conflict-free Replicated Data Types (CRDTs) over operation contexts. It cleanly separates specification-level semantics (SLS) from implementation-level compositional semantics (ICS) via CRDT composition and transformation rules, both expressed as executable Datalog programs.

  The framework is implemented in two Datalog engines:
  - Soufflé - for batch evaluation
  - DDlog - for incremental, interactive exploration

  ## CRDT Library                                                                                    
   
  ### Basic CRDTs                                                                                     
                  
  - Add-Wins Set (awset.dl) - Concurrent add/del resolved in favor of add
  - LWW Register (lwwregister.dl) - Last-writer-wins conflict resolution via arbitration order             
   
  ### Composite CRDTs                                                                                 
                  
  - Map (LWW Register) (basicMap/, basicPWMap/) - (Put-wins) Map with LWW register values                   
  - Map of Maps (mapMap/, pw_mapMap/) - Nested map composition
  - Map of Sets (mapSet/, pw_mapSet/) - Map with add-wins set values                             
                                                                                                  
  ### Application CRDTs
                                                                                                  
  Two directed graph variants, each with SLS and ICS encodings:

  - Isolate-Delete (ID) -- only isolated nodes can be removed. 
  - Detach-Delete (DD) -- node removal removes all incident edges.                                      
   
  ## Repository Structure                                                                            
                  
  souffle/          Soufflé Datalog specifications (SLS and ICS)
  ddlog/            DDlog specifications (SLS and ICS)                                            
  experiments/      Python scripts for generating operation contexts
                                                                                                  
  ### Prerequisites   

  - Soufflé (for batch validaiton)                                                                   
  - DDlog (for incremental evaluation)
  - Python (for test data generation)                                                           
                  
  ### Experiments
  The experiments directory contains scripts to generate random operation contexts (.facts files) for the graph case study:
                                                                                                  
  #### Generate abstract executions for the ID graph
  python experiments/generateAEID.py

  #### Generate abstract executions for the DD graph
  python experiments/generateAEDD.py

  #### Efficient generation for large graphs (10K+ nodes)                                            
  python experiments/efficientGraphGeneration.py
                                                                                                  
  Each script produces graphOp.facts (operations) and vis.facts (visibility relation) files.
