import re

def parse_markdown():
    with open('README.md', 'r', encoding='utf-8') as f:
        content = f.read()
    return content

def generate_html(markdown_content):
    html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stock Anomaly Pipeline - Viva Prep</title>
    <!-- Use marked.js to render markdown -->
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; background-color: #f8fafc; color: #1e293b; }
        .sidebar { position: fixed; top: 0; left: 0; width: 250px; height: 100vh; background: #0f172a; color: white; padding: 20px; overflow-y: auto; }
        .sidebar a { display: block; color: #94a3b8; text-decoration: none; padding: 10px 0; border-bottom: 1px solid #1e293b; transition: color 0.2s; }
        .sidebar a:hover { color: #38bdf8; }
        .main-content { margin-left: 250px; padding: 40px; max-width: 1000px; }
        
        .markdown-body h1 { font-size: 2.5em; font-weight: bold; margin-bottom: 0.5em; color: #0f172a; border-bottom: 2px solid #e2e8f0; padding-bottom: 10px; }
        .markdown-body h2 { font-size: 2em; font-weight: bold; margin-top: 1.5em; margin-bottom: 0.5em; color: #1e293b; }
        .markdown-body h3 { font-size: 1.5em; font-weight: bold; margin-top: 1.2em; margin-bottom: 0.5em; }
        .markdown-body p { margin-bottom: 1em; line-height: 1.6; }
        .markdown-body ul, .markdown-body ol { margin-bottom: 1em; padding-left: 2em; list-style: disc;}
        .markdown-body code { background-color: #f1f5f9; padding: 0.2em 0.4em; border-radius: 4px; font-family: monospace; font-size: 0.9em; color: #be123c;}
        .markdown-body pre { background-color: #1e293b; color: #f8fafc; padding: 1em; border-radius: 8px; overflow-x: auto; margin-bottom: 1em; }
        .markdown-body pre code { background-color: transparent; color: inherit; padding: 0; }
        .markdown-body blockquote { border-left: 4px solid #cbd5e1; padding-left: 1em; color: #475569; font-style: italic; margin-bottom: 1em; }
        .markdown-body table { width: 100%; border-collapse: collapse; margin-bottom: 1em; }
        .markdown-body th, .markdown-body td { border: 1px solid #cbd5e1; padding: 8px; text-align: left; }
        .markdown-body th { background-color: #f1f5f9; font-weight: bold; }
        
        /* Interactive Q&A styling */
        .qa-block { background: white; border: 1px solid #e2e8f0; border-radius: 8px; padding: 15px; margin-bottom: 15px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); cursor: pointer; transition: transform 0.1s;}
        .qa-block:hover { transform: translateY(-2px); box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .qa-question { font-weight: bold; font-size: 1.1em; color: #0369a1; }
        .qa-answer { margin-top: 10px; display: none; color: #334155; padding-top: 10px; border-top: 1px dashed #e2e8f0; }
        
    </style>
</head>
<body>

    <div class="sidebar" id="sidebar">
        <h2 class="text-xl font-bold mb-6 text-white">Project Viva Guide</h2>
        <a href="#1-what-you-are-building">1. What You Are Building</a>
        <a href="#2-system-architecture">2. System Architecture</a>
        <a href="#3-technology-stack">3. Technology Stack</a>
        <a href="#4-project-files-explained">4. Project Files Explained</a>
        <a href="#5-step-by-step-setup--execution">5. Setup & Execution</a>
        <a href="#6-how-the-code-works">6. How the Code Works</a>
        <a href="#7-pdc-course-concepts">7. PDC Course Concepts</a>
        <a href="#8-viva-qa-preparation" style="color: #38bdf8; font-weight: bold;">8. Viva Q&A (Interactive)</a>
        <a href="#9-port--service-reference">9. Port Reference</a>
        <a href="#10-troubleshooting">10. Troubleshooting</a>
    </div>

    <div class="main-content">
        <div id="content" class="markdown-body"></div>
    </div>

    <!-- The markdown content is injected here safely and processed by marked.js -->
    <script id="md-content" type="text/markdown">""" + markdown_content.replace('</script>', '<\\/script>') + """</script>
    
    <script>
        // Render Markdown
        const mdText = document.getElementById('md-content').textContent;
        document.getElementById('content').innerHTML = marked.parse(mdText);
        
        // Make Q&A Interactive
        setTimeout(() => {
            const content = document.getElementById('content');
            
            // Find the Viva Q&A section
            let html = content.innerHTML;
            
            // A simple regex approach to find Q: and A: patterns and wrap them in clickable blocks
            const qaRegex = /<p><strong>Q: (.*?)<\\/strong>\\s*<br>\\s*A: (.*?)<\\/p>/gs;
            
            html = html.replace(qaRegex, (match, q, a) => {
                return `
                <div class="qa-block" onclick="this.querySelector('.qa-answer').style.display = this.querySelector('.qa-answer').style.display === 'block' ? 'none' : 'block'">
                    <div class="qa-question">💡 Q: ${q}</div>
                    <div class="qa-answer">${a}</div>
                </div>
                `;
            });
            
            content.innerHTML = html;
        }, 100);
    </script>
</body>
</html>"""
    with open('interactive_readme.html', 'w', encoding='utf-8') as f:
        f.write(html_template)

if __name__ == '__main__':
    md = parse_markdown()
    generate_html(md)
    print("Generated interactive_readme.html")
