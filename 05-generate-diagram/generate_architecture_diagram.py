# diagram_generator_final.py

import pandas as pd
import graphviz
from pathlib import Path
from collections import defaultdict
import re

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# !!! CRITICAL: Update this path if necessary
TRACEABILITY_FILE = Path("../data/output_diagrams/FINAL_TRACEABILITY_REPORT.xlsx")
OUTPUT_FOLDER = Path("./diagram_output")
OUTPUT_FILE_NAME = "MBSE_Context_Diagram_Final_Normalized"

OUTPUT_FOLDER.mkdir(exist_ok=True)
CONSUMPTION_COLOR = '#D32F2F' # Red for consumed flows
EMISSION_COLOR = '#388E3C'  # Green for emitted flows

# ==============================================================================

def normalize_name(name):
    """Replaces underscores with spaces and converts to uppercase for standardization."""
    if isinstance(name, str):
        return name.strip().replace('_', ' ').upper()
    return name

def calculate_node_size(fct_tag, consumption_edges, emission_edges):
    """
    Calculate node dimensions based on number of flows.
    Returns (width, height) as strings.
    """
    # Count total flows for this FCT
    total_flows = 0
    
    # Count as source (emissions)
    for (src, tgt), fluxes in emission_edges.items():
        if src == fct_tag:
            total_flows += len(fluxes)
    
    # Count as target (consumptions)
    for (src, tgt), fluxes in consumption_edges.items():
        if tgt == fct_tag:
            total_flows += len(fluxes)
    
    # Base size + scaling factor
    base_width = 4.0          # INCREASED from 3.5
    base_height = 2.0         # INCREASED from 1.5
    
    # Add 0.6 width and 0.25 height per flow (INCREASED multipliers)
    width = base_width + (total_flows * 0.6)
    height = base_height + (total_flows * 0.25)
    
    # Cap maximum size to prevent giant boxes
    width = min(width, 15.0)
    height = min(height, 8.0)
    
    return f"{width:.1f}", f"{height:.1f}"

def generate_mbse_context_diagram_final():
    """
    Generates a Context Diagram with all requested logic updates:
    - Multi-word Subsystem extraction from 'Found In FCT' before "BLACK BOX".
    - Names normalized for consistent grouping.
    - Unique fluxes listed only once per edge.
    - Flows grouped and styled by direction (Emission/Consumption).
    - Dynamic node sizing based on flow count.
    - DOT layout with orthogonal lines and visible clusters.
    """
    print("--- MBSE Context Diagram Generator (Final Version - Dynamic Sizing) ---")
    
    if not TRACEABILITY_FILE.exists():
        print(f"ERROR: Traceability file not found at {TRACEABILITY_FILE.resolve()}")
        return

    # 1. Load Data and Filter
    try:
        df = pd.read_excel(TRACEABILITY_FILE) 
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    df_connections = df[df['Status'].str.contains('FOUND', na=False)].copy()
    
    if df_connections.empty:
        print("No successful ('FOUND') connections found. Exiting.")
        return

    print(f"Loaded {len(df_connections)} successful connections.")
    
    # 2. Map Functions to Subsystems with BLACK BOX Logic and Normalization
    fct_to_ss = defaultdict(set)
    
    def extract_subsystem_name(row):
        """
        Extracts the best subsystem name.
        Prioritizes multi-word extraction before "BLACK BOX" in 'Found In FCT'.
        """
        fct_field = str(row['Found In FCT']).strip() if 'Found In FCT' in row and pd.notna(row['Found In FCT']) else ''
        
        # 1. Check for BLACK BOX pattern in the Found In FCT column
        if 'BLACK BOX' in fct_field:
            # Capture all characters/words/spaces before "BLACK BOX" (non-greedy)
            match = re.search(r'(.+?)\s+BLACK BOX', fct_field)
            if match:
                # Return the captured group and normalize it (e.g., "CARBODY VISIBILITY")
                return normalize_name(match.group(1)) 
            
            # Fallback for BLACK BOX if pattern above fails
            if fct_field == 'BLACK BOX' and pd.notna(row['Found In Subsystem']):
                return normalize_name(row['Found In Subsystem'])

        # 2. Fallback to the dedicated Subsystem column
        ss_col_name = 'Found In Subsystem'
        if ss_col_name in row and pd.notna(row[ss_col_name]):
            return normalize_name(row[ss_col_name])
        
        # 3. Final fallback
        return "PRIMARY SYSTEM UNDER TEST"

    # Iterate over connections to build the FCT -> SS map
    for _, row in df_connections.iterrows():
        # A. Map the consumer FCT ('Found In FCT')
        consumer_fct = row['Found In FCT']
        consumer_ss = extract_subsystem_name(row)
        fct_to_ss[consumer_fct].add(consumer_ss)
        
        # B. Map the emitter FCT ('FCT')
        emitter_fct = row['FCT']
        
        # The emitter FCT's SS is taken from the Found In Subsystem column
        if emitter_fct not in fct_to_ss and pd.notna(row['Found In Subsystem']):
            emitter_ss = normalize_name(row['Found In Subsystem'])
            fct_to_ss[emitter_fct].add(emitter_ss)
        
        if emitter_fct not in fct_to_ss:
             fct_to_ss[emitter_fct].add("PRIMARY SYSTEM UNDER TEST")

    # Simplify: assume one FCT belongs to one SS (taking the first SS found)
    fct_to_ss = {fct: list(sss)[0] for fct, sss in fct_to_ss.items()}
    all_subsystems = set(fct_to_ss.values())
    
    print(f"Detected {len(all_subsystems)} Subsystems after applying BLACK BOX logic.")

    # 3. Initialize Graphviz with DOT engine and MAXIMUM spacing for labels
    diagram = graphviz.Digraph(
        'MBSE_Context_Diagram', 
        comment='System Context Diagram', 
        engine='dot',  # Keep DOT for clusters and ortho lines
        graph_attr={
             'dpi': '400',
            'rankdir': 'LR',
            'splines': 'ortho',      # Orthogonal lines
            'nodesep': '11.0',        # MAXIMUM horizontal spacing between nodes (was 3.5)
            'ranksep': '11.0',        # MAXIMUM vertical spacing between ranks (was 6.0)
            'pad': '3.0',            # MAXIMUM padding around diagram (was 1.2)
            'bgcolor': 'white',
            'concentrate': 'false',
            'sep': '+50'             # MAXIMUM separation (was +35)
        }
    )

    diagram.attr('node', shape='box', style='filled', fillcolor='#E0E0E0', 
                 fontname='Arial', margin='0.6,0.4')

    # 4. Group Flows by Direction and ensure UNIQUENESS (BEFORE NODE CREATION)
    consumption_edges = defaultdict(set) # Use a SET to ensure unique fluxes
    emission_edges = defaultdict(set)    # Use a SET to ensure unique fluxes

    for _, row in df_connections.iterrows():
        flux_name = row['Flow']
        direction = row['Direction']
        
        if direction == 'EMISSION':
            source_fct = row['FCT']
            target_fct = row['Found In FCT']
            emission_edges[(source_fct, target_fct)].add(flux_name)
        else: # CONSUMPTION
            source_fct = row['Found In FCT']
            target_fct = row['FCT']
            consumption_edges[(source_fct, target_fct)].add(flux_name)

    # 5. Create Subsystem Clusters with DYNAMIC NODE SIZING
    for ss_name in all_subsystems:
        cluster = graphviz.Digraph(
            f'cluster_{ss_name.replace(" ", "_")}', # Use normalized name for cluster ID
            graph_attr={
                'dpi': '400',
                'label': ss_name,
                'style': 'filled,rounded', 
                'color': '#1E88E5', 
                'fillcolor': '#E3F2FD', 
                'fontname': 'Arial Bold',
                'fontsize': '16',        # Increased font size
                'penwidth': '2.5',       # Thicker border
                'margin': '50'           # MAXIMUM margin inside cluster (was 35)
            }
        )
        
        fcts_in_ss = [fct for fct, ss in fct_to_ss.items() if ss == ss_name]
        
        for fct_tag in fcts_in_ss:
            # Use the normalized name for display in the node label
            label_text = normalize_name(fct_tag).replace('FCT ', '')
            
            # Calculate dynamic size based on flow count
            width, height = calculate_node_size(fct_tag, consumption_edges, emission_edges)
            
            cluster.node(
                fct_tag, # Use original FCT tag as the unique ID for edges
                label=label_text,
                width=width,          # DYNAMIC WIDTH
                height=height,        # DYNAMIC HEIGHT
                fixedsize='true',
                fontsize='11',
                margin='1.0,0.6'      # MAXIMUM internal padding (was 0.8,0.5)
            )
            
        diagram.subgraph(cluster)

    # 6. DRAW CONSUMPTION EDGES FIRST (with red styling and spacing)
    print(f"\n📥 Drawing {len(consumption_edges)} CONSUMPTION connections...")
    
    diagram.attr('edge', fontname='Arial', fontsize='10', 
                 color=CONSUMPTION_COLOR, arrowhead='vee', penwidth='2.5')
    
    for (source, target), fluxes_set in consumption_edges.items():
        fluxes = sorted(list(fluxes_set)) # Convert set to list and sort
        flux_list = '📥 CONSUMED:\n' + '\n'.join([f'  • {f}' for f in fluxes])
        
        diagram.edge(
            source, 
            target, 
            xlabel=flux_list, 
            minlen='6',              # MAXIMUM minimum edge length (was 4)
            labelfontcolor=CONSUMPTION_COLOR,
            labelfontsize='10',
            weight='1'
        )

    # 7. DRAW EMISSION EDGES SECOND (with green styling and spacing)
    print(f"📤 Drawing {len(emission_edges)} EMISSION connections...")
    
    diagram.attr('edge', fontname='Arial', fontsize='10', 
                 color=EMISSION_COLOR, arrowhead='vee', penwidth='2.5')
    
    for (source, target), fluxes_set in emission_edges.items():
        fluxes = sorted(list(fluxes_set)) # Convert set to list and sort
        flux_list = '📤 EMITTED:\n' + '\n'.join([f'  • {f}' for f in fluxes])
        
        diagram.edge(
            source, 
            target, 
            xlabel=flux_list, 
            minlen='6',              # MAXIMUM minimum edge length (was 4)
            labelfontcolor=EMISSION_COLOR,
            labelfontsize='10',
            weight='2'
        )

    # 8. Add Legend
    legend = graphviz.Digraph('cluster_legend')
    legend.attr(label='LEGEND', style='filled', fillcolor='#FFFDE7', 
                 fontname='Arial Bold', fontsize='12')
    
    legend.node('legend_consumption', 'Consumption Flow', 
                 shape='plaintext', fontcolor=CONSUMPTION_COLOR, fontsize='10')
    legend.node('legend_emission', 'Emission Flow', 
                 shape='plaintext', fontcolor=EMISSION_COLOR, fontsize='10')
    
    diagram.subgraph(legend)

    # 9. Render Output
    output_path = OUTPUT_FOLDER / OUTPUT_FILE_NAME
    diagram.render(output_path, view=False, format='png')

    print("\n✅ Diagram Generation Complete!")
    print(f"   - Output: {output_path.resolve()}.png")
    print(f"\n📊 Statistics:")
    print(f"   - Consumption connections: {len(consumption_edges)}")
    print(f"   - Emission connections: {len(emission_edges)}")
    print(f"   - Total flows traced: {len(df_connections)}")


if __name__ == '__main__':
    generate_mbse_context_diagram_final()