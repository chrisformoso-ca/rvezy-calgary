# RVezy Dashboard Deployment Guide

## Overview
The RVezy Calgary Market Intelligence dashboard is a static site that can be deployed to any web hosting service. It consists of just two files that work together client-side.

## Files to Deploy
- `index.html` - The main dashboard interface (formerly dashboard_comprehensive.html)
- `comprehensive_dashboard_data.json` - Pre-generated data file (621KB)

## Deployment Options

### Option 1: GitHub Pages (Recommended)

1. **Create a new branch for deployment:**
   ```bash
   git checkout -b gh-pages
   ```

2. **Copy the output files to root:**
   ```bash
   cp output/index.html .
   cp output/comprehensive_dashboard_data.json .
   ```

3. **Commit and push:**
   ```bash
   git add index.html comprehensive_dashboard_data.json
   git commit -m "Deploy dashboard to GitHub Pages"
   git push origin gh-pages
   ```

4. **Enable GitHub Pages:**
   - Go to Settings → Pages in your GitHub repository
   - Source: Deploy from a branch
   - Branch: gh-pages / root
   - Save

5. **Access your dashboard:**
   - URL: `https://chrisformoso-ca.github.io/rvezy-calgary/`

### Option 2: Deploy to Main Branch

1. **Move files to docs folder:**
   ```bash
   mkdir -p docs
   cp output/index.html docs/
   cp output/comprehensive_dashboard_data.json docs/
   ```

2. **Commit to main branch:**
   ```bash
   git add docs/
   git commit -m "Add dashboard to docs folder"
   git push origin main
   ```

3. **Configure GitHub Pages:**
   - Settings → Pages
   - Source: Deploy from a branch
   - Branch: main / docs
   - Save

### Option 3: Netlify Drop

1. Visit https://app.netlify.com/drop
2. Drag and drop the `output` folder containing both files
3. Netlify will instantly deploy and provide a URL

### Option 4: Local Network (Proxmox)

Since you have a Proxmox server, you can host it locally:

1. **Install a web server in a container:**
   ```bash
   # Create LXC container with Ubuntu/Debian
   # Install nginx
   apt update && apt install nginx -y
   ```

2. **Copy files to web root:**
   ```bash
   scp output/index.html output/comprehensive_dashboard_data.json root@proxmox-ip:/var/www/html/
   ```

3. **Access from any device on your network:**
   - URL: `http://proxmox-ip/`

## Updating the Dashboard

To update with fresh data:

1. **Generate new data:**
   ```bash
   python3 scripts/extract_rvezy_data.py  # If you have new CSV data
   python3 scripts/generate_comprehensive_dashboard.py
   ```

2. **Prepare for deployment:**
   ```bash
   cp output/dashboard_comprehensive.html output/index.html
   ```

3. **Re-deploy using your chosen method**

## Making it a Progressive Web App (PWA)

To enable offline access and "install to homescreen":

1. **Create manifest.json:**
   ```json
   {
     "name": "RVezy Calgary Market Intelligence",
     "short_name": "RVezy Intel",
     "start_url": "/",
     "display": "standalone",
     "theme_color": "#667eea",
     "background_color": "#f0f2f5",
     "icons": [
       {
         "src": "icon-192.png",
         "sizes": "192x192",
         "type": "image/png"
       }
     ]
   }
   ```

2. **Add to index.html head:**
   ```html
   <link rel="manifest" href="manifest.json">
   <meta name="theme-color" content="#667eea">
   ```

3. **Create a simple service worker (sw.js):**
   ```javascript
   self.addEventListener('install', e => {
     e.waitUntil(
       caches.open('rvezy-v1').then(cache => {
         return cache.addAll([
           '/',
           '/index.html',
           '/comprehensive_dashboard_data.json'
         ]);
       })
     );
   });

   self.addEventListener('fetch', e => {
     e.respondWith(
       caches.match(e.request).then(response => {
         return response || fetch(e.request);
       })
     );
   });
   ```

4. **Register service worker in index.html:**
   ```javascript
   if ('serviceWorker' in navigator) {
     navigator.serviceWorker.register('/sw.js');
   }
   ```

## Security Considerations

- The dashboard contains market intelligence data that may be sensitive
- Consider adding basic authentication if hosting publicly
- For GitHub Pages, you can use a private repository with GitHub Pro/Team
- For local hosting, use firewall rules to restrict access

## Performance Tips

- The data file is 621KB - consider using gzip compression on your web server
- Enable browser caching headers for the JSON file
- The dashboard loads external libraries (Plotly, jQuery) from CDNs

## Troubleshooting

1. **"Cannot read property 'toFixed' of null"**
   - Regenerate data with: `python3 scripts/generate_comprehensive_dashboard.py`

2. **Charts not displaying**
   - Check browser console for errors
   - Ensure comprehensive_dashboard_data.json is in the same directory as index.html

3. **404 errors**
   - Verify both files are uploaded
   - Check file names match exactly (case-sensitive)

## Mobile Access

The dashboard is fully responsive and works on:
- iOS Safari (iPhone/iPad)
- Android Chrome
- Any modern mobile browser

For best mobile experience:
- Add to homescreen for app-like experience
- Use landscape mode on phones for better chart visibility
- All tables are scrollable horizontally