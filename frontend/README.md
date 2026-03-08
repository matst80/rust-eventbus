# EventBus Todo Frontend

A clean, GitHub Issues-inspired frontend for the Rust EventBus Todo app.

## Tech Stack
- **Vite** + **React**
- **Tailwind CSS v4** (Modern CSS-first approach)
- **Lucide React** for icons
- **Vite Proxy** for API calls

## Setup & Running

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Run in development**:
   ```bash
   npm run dev
   ```
   The app will be available at `http://localhost:5173`. API calls to `/api/*` will be proxied to `https://todo.k6n.net/*`.

3. **Build for production**:
   ```bash
   npm run build
   ```

## Features
- GitHub Dark Mode aesthetics.
- Responsive design.
- Full CRUD integration with the EventBus backend.
- Proxying to the deployed Kubernetes host.
