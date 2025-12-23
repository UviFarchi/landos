<template>
  <button data-test="layer-toggle" type="button" @click="toggle">
    {{ label }}
  </button>
</template>

<script>
export default {
  name: 'LayerToggle',
  props: {
    label: { type: String, default: '' },
    modelValue: { type: Boolean, default: false },
  },
  emits: ['update:modelValue'],
  methods: {
    toggle() {
      this.$emit('update:modelValue', !this.modelValue);
    },
  },
};
</script>
