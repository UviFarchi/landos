import { createRouter, createWebHistory } from 'vue-router';
import Landing from '../views/Landing.vue';
import Projects from '../views/Projects.vue';
import Selection from '../views/Selection.vue';
import Main from '../views/Main.vue';

const routes = [
  { path: '/', name: 'Landing', component: Landing },
  { path: '/projects', name: 'Projects', component: Projects },
  { path: '/selection', name: 'Selection', component: Selection },
  { path: '/projects/:id', name: 'Project', component: Main, props: true },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

export default router;
