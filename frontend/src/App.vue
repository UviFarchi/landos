<template>
  <div id="app">
    <router-view />
  </div>
</template>

<script>
export default {
  name: 'App',
};
</script>

<style>
* {
  box-sizing: border-box;
}

body {
  margin: 0;
  font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
  background: #0f172a;
  color: #e2e8f0;
}

#app {
  min-height: 100vh;
}

a {
  color: inherit;
  text-decoration: none;
}
</style>
