<template>
  <div class="landing">
    <header class="hero">
      <div>
        <h1>LandOS</h1>
        <p class="subtitle">Sign in to manage your projects on-site.</p>
      </div>
    </header>

    <section class="auth">
      <div class="tabs">
        <button :class="{active: mode === 'signin'}" @click="mode = 'signin'">Sign in</button>
        <button :class="{active: mode === 'signup'}" @click="mode = 'signup'">Sign up</button>
      </div>

      <form class="card" v-if="mode === 'signin'" @submit.prevent="submit('signin')">
        <h3>Sign in</h3>
        <label>
          Username
          <input v-model="auth.username" required />
        </label>
        <label>
          Password
          <input v-model="auth.password" type="password" required />
        </label>
        <button class="solid" type="submit">Sign in</button>
        <p v-if="status" class="status">{{ status }}</p>
      </form>

      <form class="card" v-else @submit.prevent="submit('signup')">
        <h3>Create account</h3>
        <label>
          Username
          <input v-model="signup.username" required />
        </label>
        <label>
          Password
          <input v-model="signup.password" type="password" required />
        </label>
        <button class="ghost" type="submit">Sign up</button>
        <p v-if="signupStatus" class="status">{{ signupStatus }}</p>
      </form>
    </section>
  </div>
</template>

<script>
const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000';

export default {
  name: 'LandingView',
  data() {
    return {
      mode: 'signin',
      auth: { username: '', password: '' },
      signup: { username: '', password: '' },
      status: '',
      signupStatus: '',
    };
  },
  methods: {
    async submit(kind) {
      const payload = kind === 'signin' ? this.auth : this.signup;
      if (payload.username) {
        payload.username = payload.username.trim().toLowerCase();
      }
      const endpoint = kind === 'signin' ? '/api/platform/login' : '/api/platform/signup';
      const resp = await fetch(`${API_BASE}${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const body = await resp.json().catch(() => ({}));
      if (!resp.ok || body.error) {
        if (kind === 'signin') {
          this.status = body.error || 'Sign in failed';
          this.mode = 'signup';
        } else {
          this.signupStatus = body.error || 'Sign up failed';
        }
        return;
      }
      if (kind === 'signin') {
        this.status = '';
        if (body.token) {
          localStorage.setItem('token', body.token);
          localStorage.setItem('username', payload.username);
        }
        this.$router.push('/projects');
      } else {
        this.signupStatus = 'Account created. You can sign in now.';
      }
    },
  },
};
</script>

<style scoped>
.landing {
  padding: 32px;
  max-width: 960px;
  margin: 0 auto;
  color: #e2e8f0;
}
.hero h1 {
  margin: 0;
  font-size: 32px;
}
.subtitle {
  color: #94a3b8;
  margin: 6px 0 0;
}
.auth-grid {
  max-width: 420px;
  margin-top: 24px;
}
.card {
  background: #0f172a;
  border: 1px solid #1e293b;
  border-radius: 12px;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.card h3 {
  margin: 0;
}
label {
  display: flex;
  flex-direction: column;
  gap: 6px;
  color: #cbd5e1;
}
input {
  padding: 10px;
  border-radius: 8px;
  border: 1px solid #1e293b;
  background: #0b1222;
  color: #e2e8f0;
}
button {
  border: none;
  border-radius: 10px;
  padding: 10px 14px;
  font-weight: 600;
  cursor: pointer;
}
.solid {
  background: #38bdf8;
  color: #0b1222;
}
.ghost {
  background: transparent;
  color: #e2e8f0;
  border: 1px solid #334155;
}
.status {
  color: #fca5a5;
  margin: 4px 0 0;
  font-size: 14px;
}
.tabs {
  display: inline-flex;
  border: 1px solid #1e293b;
  border-radius: 10px;
  overflow: hidden;
  margin-bottom: 12px;
}
.tabs button {
  background: transparent;
  color: #cbd5e1;
  border: none;
  padding: 10px 14px;
}
.tabs button.active {
  background: #38bdf8;
  color: #0b1222;
}
</style>
